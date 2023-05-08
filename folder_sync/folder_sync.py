from pathlib import Path
from typing import List, Callable, Tuple, Dict, Union, Any
import hashlib
import logging
import concurrent.futures
import os
import shutil
from collections import defaultdict
import time
import filecmp
import enum

from tqdm import tqdm


class Action(enum.Enum):
    COPY_FILE = "copy_file"
    DELETE_FILE = "delete_file"
    DELETE_EMPTY_FOLDER = "delete_folder"
    CREATE_EMPTY_FOLDER = "create_folder"
    COMPARE_FILE = "compare_file"


class Stat(enum.Enum):
    DELETED_FOLDERS = "deleted_folders"
    DELETED_FILES = "deleted_files"
    COPIED_FOLDERS = "copied_folders"
    COPIED_FILES = "copied_files"
    COMPARED_FILES = "compared_files"
    CHANGED_FILES = "changed_files"
    UNCHANGED_FILES = "unchanged_files"


def _get_delete_folder_actions(
    folder_path: Path, actions: Dict[Action, List[Any]], stats: Dict[Stat, List[Any]]
):
    for p in folder_path.iterdir():
        if p.is_dir():
            _get_delete_folder_actions(p)
        elif p.is_file():
            actions[Action.DELETE_FILE].append(p)
            stats[Stat.DELETED_FILES].append(p)

    actions[Action.DELETE_EMPTY_FOLDER].append(folder_path)


def _get_copy_folder_actions(
    source_path,
    target_path,
    actions: Dict[Action, List[Any]],
    stats: Dict[Stat, List[Any]],
):
    actions[Action.CREATE_EMPTY_FOLDER].append(target_path)

    for p in source_path.iterdir():
        if p.is_dir():
            _get_copy_folder_actions(p, target_path / p.name, actions)
        elif p.is_file():
            actions[Action.COPY_FILE].append((p, target_path / p.name))
            stats[Stat.COPIED_FILES].append((p, target_path / p.name))


def _get_actions(
    source_dir: Path,
    target_dir: Path,
    actions: Dict[Action, List[Any]],
    stats: Dict[Stat, List[Any]],
):
    target_names = [p.name for p in target_dir.iterdir()]
    source_names = [p.name for p in source_dir.iterdir()]

    # delete files and folders which are in tagret but not in source anymore
    # we need to update the target_names list as otherwise we iterate over invalid elements in the next loop
    kept_target_names = []
    for name in target_names:
        source_path = source_dir / name
        target_path = target_dir / name
        if name not in source_names or not (
            source_path.is_file() == target_path.is_file()
            and source_path.is_dir() == target_path.is_dir()
            and source_path.exists() == target_path.exists()
        ):
            if target_path.is_file():
                stats[Stat.DELETED_FILES].append(target_path)
                actions[Action.DELETE_FILE].append(target_path)
            elif target_path.is_dir():
                stats[Stat.DELETED_FOLDERS].append(target_path)
                _get_delete_folder_actions(target_path, actions)
        else:
            kept_target_names.append(name)

    target_names = kept_target_names

    # copy files and folders which are in source but not in target
    for name in source_names:
        source_path = source_dir / name
        target_path = target_dir / name

        if name not in target_names:
            if source_path.is_file():
                actions[Action.COPY_FILE].append((source_path, target_path))
                stats[Stat.COPIED_FILES].append((source_path, target_path))
            elif source_path.is_dir():
                stats[Stat.COPIED_FOLDERS].append((source_path, target_path))
                _get_copy_folder_actions(source_path, target_path, actions)

        else:  # name exists in source and target and is of same type (file or folder)
            if source_path.is_file():
                stats[Stat.COMPARED_FILES].append((source_path, target_path))
                actions[Action.COMPARE_FILE].append((source_path, target_path))
            elif source_path.is_dir():
                _get_actions(source_path, target_path, actions)


def _run_executer_with_progress(n_threads, func, data: List[Tuple]):
    executer = concurrent.futures.ThreadPoolExecutor(max_workers=n_threads)
    futures = [executer.submit(func, *d) for d in data]

    # logging progress
    with tqdm(total=len(futures)) as pbar:
        for _ in concurrent.futures.as_completed(futures):
            pbar.update(1)

    executer.shutdown(wait=True)
    return [f.result() for f in futures]


def sync_folders(
    source_folder: Path,
    target_folder: Path,
    n_thredas: int = 1,
    shallow_comparison: bool = True,
    verbose: bool = False,
):
    """Sync two folders recursively.
    All files and folders which are in target but not in source anymore are deleted.
    All files and folders which are in source but not in target are copied.
    All files which are in both source and target are updated if their hash is different.

    Args:
        source_folder: path to the source folder
        target_folder: path to the target folder
        n_thredas: number of threads to use, default is 1
    """
    logging.info(f"Syncing {source_folder} to {target_folder}")

    start_time = time.time()

    logging.info("Detecting necessary actions...")
    actions = defaultdict(list)
    stats = defaultdict(list)
    _get_actions(source_folder, target_folder, actions, stats)

    logging.info("Comparing files...")
    results = _run_executer_with_progress
    (
        n_thredas,
        filecmp.cmp,
        actions[Action.COMPARE_FILE],
        shallow_comparison,
    )
    # executer = concurrent.futures.ThreadPoolExecutor(max_workers=n_thredas)
    # futures = [
    #     executer.submit(filecmp.cmp, f1, f2, shallow_comparison)
    #     for f1, f2 in actions[Action.COMPARE_FILE]
    # ]

    # # logging progress
    # with tqdm(total=len(futures)) as pbar:
    #     for _ in concurrent.futures.as_completed(futures):
    #         pbar.update(1)

    # update actions
    for i, (source, target) in enumerate(actions.pop(Action.COMPARE_FILE)):
        if not futures[i].result():
            actions[Action.DELETE_FILE].append(target)
            actions[Action.COPY_FILE].append((source, target))
            stats[Stat.CHANGED_FILES].append((source, target))
        else:
            stats[Stat.UNCHANGED_FILES].append((source, target))

    logging.info("The following actions will be executed:")
    for action, args in actions.items():
        logging.info(f"{action}: {args if verbose else len(args)}")

    # Do you want to continue?
    # TODO

    logging.info("Executing actions...")
    logging.info("Deleting files...")
    executer = concurrent.futures.ThreadPoolExecutor(max_workers=n_thredas)
    futures = [
        executer.submit(lambda p: p.unlink(), p) for p in actions[Action.DELETE_FILE]
    ]
    with tqdm(total=len(futures)) as pbar:
        for _ in concurrent.futures.as_completed(futures):
            pbar.update(1)

    # delete files from target
    # delete empty folders from target
    # create empty folders in target
    # copy files to target

    logging.info(
        f"Finished syncing {source_folder} to {target_folder} in {time.time() - start_time:.2f} seconds"
    )


# def _delete_outdated_element(target_path: Path):
#     if target_path.is_file():
#         target_path.unlink()
#         return "deleted_file"
#     elif target_path.is_dir():
#         # TODO do own recursion for better tracking of progress, !!! accoutn for empty folders
#         shutil.rmtree(target_path)
#         return "deleted_folder"


# def _copy_new_element(source_path: Path, target_path: Path):
#     if source_path.is_file():
#         shutil.copy2(source_path, target_path)
#         return "copied_file"
#     elif source_path.is_dir():
#         # TODO do own recursion for better tracking of progress, !!! accoutn for empty folders
#         shutil.copytree(source_path, target_path)
#         return "copied_folder"


# def _update_existing_file(
#     source_path: Path, target_path: Path, file_hash_func: Callable
# ):
#     if file_hash_func(source_path) == file_hash_func(target_path):
#         return "unchanged_file"
#     target_path.unlink()
#     shutil.copy2(source_path, target_path.parent)
#     return "changed_file"


# def aggregate_action_statistics(
#     actions: List[Tuple[Callable, Any]]
# ) -> Dict[str, List[Path]]:
#     stats = {
#         "deleted_folders": [],
#         "deleted_files": [],
#         "copied_folders": [],
#         "copied_files": [],
#         "updated_files": [],
#     }
#     for action, *args in actions:
#         if action == _delete_outdated_element:
#             p = args[0]
#             if p.is_file():
#                 stats["files_deleted"].append(args[0])
#             elif p.is_dir():
#                 stats["folders_deleted"].append(args[0].parent)
#         elif action == _copy_new_element:
#             stats["folders_copied"].append(args[1].parent)
#         elif action == _update_existing_file:
#             stats["files_updated"].append(args[1].parent)
#         else:
#             raise ValueError(f"Unknown action {action}")
