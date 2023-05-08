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


# we can associate exactly one change to each relative path
class Change(enum.Enum):
    NEW_FILE = enum.auto()
    NEW_FOLDER = enum.auto()
    CHANGED_FILE = enum.auto()
    CHANGED_FILE2FOLDER = enum.auto()
    CHANGED_FOLDER2FILE = enum.auto()
    UNCHANGED_FILE = enum.auto()
    REMOVED_FILE = enum.auto()
    REMOVED_FOLDER = enum.auto()
    INVALID_TYPE = enum.auto()
    UNCHANGED_FOLDER = enum.auto()


# multiple actions can be associated to each relative path (e.g. first delete a file, then create a folder with the same name)
class Action(enum.Enum):
    COPY_FILE = enum.auto()
    DELETE_FILE = enum.auto()
    CREATE_FOLDER = enum.auto()
    DELETE_FOLDER = enum.auto()


def _determine_change(
    rel_path: Path,
    source_folder: Path,
    target_folder: Path,
    shallow_comparison: bool,
    in_source: bool,
    in_target: bool,
):
    if not in_source and in_target:
        if target_folder / rel_path.is_file():
            return Change.REMOVED_FILE
        elif target_folder / rel_path.is_dir():
            return Change.REMOVED_FOLDER
        else:
            return Change.INVALID_TYPE

    if in_source and not in_target:
        if source_folder / rel_path.is_file():
            return Change.NEW_FILE
        elif source_folder / rel_path.is_dir():
            return Change.NEW_FOLDER
        else:
            return Change.INVALID_TYPE

    if in_source and in_target:
        if source_folder / rel_path.is_file() and target_folder / rel_path.is_file():
            if filecmp(
                source_folder / rel_path, target_folder / rel_path, shallow_comparison
            ):
                return Change.UNCHANGED_FILE
            else:
                return Change.CHANGED_FILE
        elif source_folder / rel_path.is_file() and target_folder / rel_path.is_dir():
            return Change.CHANGED_FILE2FOLDER
        elif source_folder / rel_path.is_dir() and target_folder / rel_path.is_file():
            return Change.CHANGED_FOLDER2FILE
        elif source_folder / rel_path.is_dir() and target_folder / rel_path.is_dir():
            return Change.UNCHANGED_FOLDER
        else:
            return Change.INVALID_TYPE


def _run_executer_with_progress(n_threads, func, data: List[Tuple]):
    executer = concurrent.futures.ThreadPoolExecutor(max_workers=n_threads)
    futures = [executer.submit(func, *d) for d in data]

    # logging progress
    with tqdm(total=len(futures)) as pbar:
        for _ in concurrent.futures.as_completed(futures):
            pbar.update(1)

    executer.shutdown(wait=True)
    return [f.result() for f in futures]


def _infer_actions(all_paths, all_changes):
    actions = {e: [] for e in Action}
    for change_type, path in zip(all_changes, all_paths):
        if change_type == Change.REMOVED_FILE:
            actions[Action.DELETE_FILE].append(path)
        elif change_type == Change.REMOVED_FOLDER:
            actions[Action.DELETE_FOLDER].append(path)
        elif change_type == Change.NEW_FILE:
            actions[Action.COPY_FILE].append(path)
        elif change_type == Change.NEW_FOLDER:
            actions[Action.CREATE_FOLDER].append(path)
        elif change_type == Change.CHANGED_FILE:
            actions[Action.DELETE_FILE].append(path)
            actions[Action.COPY_FILE].append(path)
        elif change_type == Change.CHANGED_FILE2FOLDER:
            actions[Action.DELETE_FILE].append(path)
            actions[Action.CREATE_FOLDER].append(path)
        elif change_type == Change.CHANGED_FOLDER2FILE:
            actions[Action.DELETE_FOLDER].append(path)
            actions[Action.COPY_FILE].append(path)

    return actions


def sync_folders(
    source_folder: Path,
    target_folder: Path,
    n_thredas: int = 1,
    shallow_comparison: bool = True,
    verbose_logging: bool = False,
):
    logging.info(f"Syncing {source_folder} to {target_folder}")
    start_time = time.time()

    logging.info("Detecting paths...")
    source_paths = {p.relative_to(source_folder) for p in source_folder.rglob("*")}
    target_paths = {p.relative_to(target_folder) for p in target_folder.rglob("*")}
    # use list to have defined order
    all_paths = list(source_paths | target_paths)

    logging.info("Determining changes...")
    change_results = _run_executer_with_progress(
        n_thredas,
        _determine_change,
        [
            (
                rel_path,
                source_folder,
                target_folder,
                shallow_comparison,
                rel_path in source_paths,
                rel_path in target_paths,
            )
            for rel_path in all_paths
        ],
    )
    changes = {c: [] for c in Change}
    for change_type, rel_path in zip(change_results, all_paths):
        changes[change_type].append(rel_path)

    # check for invalid types
    if len(changes[Change.INVALID_TYPE]) > 0:
        invalid_paths_str = "\n".join([str(p) for p in changes[Change.INVALID_TYPE]])
        logging.warning(
            f"The following elements were ignored because they are neither files nor folders. This can lead to unexpected behavior:\n{invalid_paths_str}"
        )
        logging.warning("Do you want to continue? (y/n)")
        if input("y/n: ") != "y":
            logging.info("Aborting...")
            return

    logging.info("Inferring actions...")
    actions = _infer_actions(all_paths, change_results)

    logging.info(f"{len(changes[Change.UNCHANGED_FILE])} are unchanged.")
    logging.info("The following actions will be applied on the target folder:")
    for action, paths in actions.items():
        info_str = f"{action.name}: {len(paths)}"
        if action in (Action.CREATE_FOLDER, Action.DELETE_FOLDER):
            info_str += (
                f" ({len([p for p in paths if p.parent not in paths])} top-level)"
            )
        logging.info(info_str)
        if verbose_logging:
            logging.info("\n".join([str(p) for p in paths]))

    logging.info("Do you want to continue? (y/n)")
    if input("y/n: ") != "y":
        logging.info("Aborting...")
        return

    logging.info("Applying changes...")
    logging.info("Deleting files...")
    _run_executer_with_progress(
        n_thredas,
        lambda rel_path: (target_folder / rel_path).unlink(),
        actions[Action.DELETE_FILE],
    )
    logging.info("Deleting (now empty) folders...")
    _run_executer_with_progress(
        n_thredas,
        lambda rel_path: (target_folder / rel_path).rmdir(),
        actions[Action.DELETE_FOLDER],
    )
    logging.info("Creating folders...")
    _run_executer_with_progress(
        n_thredas,
        lambda rel_path: (target_folder / rel_path).mkdir(parents=True, exists_ok=True),
        actions[Action.CREATE_FOLDER],
    )
    logging.info("Copying files...")
    _run_executer_with_progress(
        n_thredas,
        lambda rel_path: shutil.copy2(
            source_folder / rel_path, target_folder / rel_path
        ),
        actions[Action.COPY_FILE],
    )

    logging.info(
        f"Finished syncing {source_folder} to {target_folder} in {time.time() - start_time:.2f} seconds"
    )
