from pathlib import Path
from typing import List, Tuple, Set, Callable, Any, Dict
import logging
import concurrent.futures
import shutil
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
    UNCHANGED_FOLDER = enum.auto()


# multiple actions can be associated to each relative path (e.g. first delete a file, then create a folder with the same name)
class Action(enum.Enum):
    COPY_FILE = enum.auto()
    DELETE_FILE = enum.auto()
    CREATE_FOLDER = enum.auto()
    DELETE_FOLDER = enum.auto()


def _run_executer_with_progress(
    n_threads: int, func: Callable, data: List[Tuple]
) -> List[Any]:
    executer = concurrent.futures.ThreadPoolExecutor(max_workers=n_threads)
    futures = [executer.submit(func, *d) for d in data]

    with tqdm(total=len(futures)) as pbar:
        for _ in concurrent.futures.as_completed(futures):
            pbar.update(1)

    executer.shutdown(wait=True)

    return [f.result() for f in futures]


def _handle_invlid_types(
    source_paths: Set[Path], target_paths: Set[Path]
) -> Tuple[Set[Path], Set[Path]]:
    invalid_source_paths = {
        p for p in source_paths if not p.is_file() and not p.is_dir()
    }
    if invalid_source_paths:
        logging.warning(
            "The following paths are neither files or directories and therefore wont be synced"
        )
        for p in invalid_source_paths:
            logging.warning(p)

    invalid_target_paths = {
        p for p in target_paths if not p.is_file() and not p.is_dir()
    }
    if invalid_target_paths:
        logging.warning(
            "The following paths in the target folder are neither files or directories. The tool cant handle them so they need to be removed before continuing."
        )
        for p in invalid_target_paths:
            logging.warning(p)
        logging.warning("Do you want to continue?")
        if input("y/n: ") != "y":
            logging.info("Aborting...")
            exit()
        for p in invalid_target_paths:
            # try:
            p.unlink()
            # except OSError as e:
            #     logging.fatal(f"Couldnt delete {p}")
            #     raise e

        return invalid_source_paths, invalid_target_paths


def _determine_change(
    rel_path: Path,
    source_folder: Path,
    target_folder: Path,
    shallow_comparison: bool,
    in_source: bool,
    in_target: bool,
) -> Change:
    if not in_source and in_target:
        if target_folder / rel_path.is_file():
            return Change.REMOVED_FILE
        elif target_folder / rel_path.is_dir():
            return Change.REMOVED_FOLDER
        else:
            assert False, "There shouldnt be any paths which arent files or folder"

    if in_source and not in_target:
        if source_folder / rel_path.is_file():
            return Change.NEW_FILE
        elif source_folder / rel_path.is_dir():
            return Change.NEW_FOLDER
        else:
            assert False, "There shouldnt be any paths which arent files or folder"

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
            assert False, "There shouldnt be any paths which arent files or folder"


def _get_changes(
    n_threads: int,
    source_folder: Path,
    target_folder: Path,
    shallow_comparison: bool,
    source_paths_rel: Set[Path],
    target_paths_rel: Set[Path],
    all_paths_rel: Set[Path],
) -> Dict[Change : List[Path]]:
    all_paths_rel = list(all_paths_rel)
    change_results = _run_executer_with_progress(
        n_threads,
        _determine_change,
        [
            (
                rel_path,
                source_folder,
                target_folder,
                shallow_comparison,
                rel_path in source_paths_rel,
                rel_path in target_paths_rel,
            )
            for rel_path in all_paths_rel
        ],
    )
    changes = {c: [] for c in Change}
    for change_type, rel_path in zip(change_results, all_paths_rel):
        changes[change_type].append(rel_path)

    return changes


def _infer_actions(changes: Dict[Change : List[Path]]) -> Dict[Action, List[Path]]:
    actions = {e: [] for e in Action}
    for change_type, paths in changes.items():
        if change_type == Change.REMOVED_FILE:
            actions[Action.DELETE_FILE].extend(paths)
        elif change_type == Change.REMOVED_FOLDER:
            actions[Action.DELETE_FOLDER].extend(paths)
        elif change_type == Change.NEW_FILE:
            actions[Action.COPY_FILE].extend(paths)
        elif change_type == Change.NEW_FOLDER:
            actions[Action.CREATE_FOLDER].extend(paths)
        elif change_type == Change.CHANGED_FILE:
            actions[Action.DELETE_FILE].extend(paths)
            actions[Action.COPY_FILE].extend(paths)
        elif change_type == Change.CHANGED_FILE2FOLDER:
            actions[Action.DELETE_FILE].extend(paths)
            actions[Action.CREATE_FOLDER].extend(paths)
        elif change_type == Change.CHANGED_FOLDER2FILE:
            actions[Action.DELETE_FOLDER].extend(paths)
            actions[Action.COPY_FILE].extend(paths)

    return actions


def _log_actions(
    actions: Dict[Change, List[Path]], changes: Dict[Action, List[Path]], verbose: bool
):
    logging.info(f"{len(changes[Change.UNCHANGED_FILE])} are unchanged.")
    for action, paths in actions.items():
        info_str = f"{action.name}: {len(paths)}"
        if action in (Action.CREATE_FOLDER, Action.DELETE_FOLDER):
            info_str += (
                f" ({len([p for p in paths if p.parent not in paths])} top-level)"
            )
        logging.info(info_str)
        if verbose:
            logging.info("\n".join([str(p) for p in paths]))

    logging.info("Do you want to continue? (y/n)")
    if input("y/n: ") != "y":
        logging.info("Aborting...")
        exit()


def sync_folders(
    source_folder: Path,
    target_folder: Path,
    n_threads: int = 1,
    shallow_comparison: bool = True,
    verbose_logging: bool = False,
):
    logging.info(f"Syncing {source_folder} to {target_folder}")
    start_time = time.time()

    logging.info("Detecting paths...")
    source_paths = source_folder.rglob("*")
    target_paths = target_folder.rglob("*")

    logging.info("Checking for invalid types")
    invalid_source_paths, invalid_target_paths = _handle_invlid_types(
        source_paths, target_paths
    )
    source_paths = source_paths - invalid_source_paths
    target_paths = target_paths - invalid_target_paths

    logging.info("Making relative paths...")
    source_paths_rel = {p.relative_to(source_folder) for p in source_folder.rglob("*")}
    target_paths_rel = {p.relative_to(target_folder) for p in target_folder.rglob("*")}
    all_paths_rel = source_paths_rel | target_paths_rel

    logging.info("Determining changes...")
    changes: Dict[Change, List[Path]] = _get_changes(
        n_threads,
        source_folder,
        target_folder,
        shallow_comparison,
        source_paths_rel,
        target_paths_rel,
        all_paths_rel,
    )

    logging.info("Inferring actions...")
    actions: Dict[Action, List[Path]] = _infer_actions(changes)

    logging.info("The following actions will be applied on the target folder:")
    _log_actions(actions, changes, verbose_logging)

    logging.info("Applying changes...")
    logging.info("Deleting files...")
    _run_executer_with_progress(
        n_threads,
        lambda rel_path: (target_folder / rel_path).unlink(),
        actions[Action.DELETE_FILE],
    )
    logging.info("Deleting (now empty) folders...")
    _run_executer_with_progress(
        n_threads,
        lambda rel_path: (target_folder / rel_path).rmdir(),
        actions[Action.DELETE_FOLDER],
    )
    logging.info("Creating folders...")
    _run_executer_with_progress(
        n_threads,
        lambda rel_path: (target_folder / rel_path).mkdir(parents=True, exists_ok=True),
        actions[Action.CREATE_FOLDER],
    )
    logging.info("Copying files...")
    _run_executer_with_progress(
        n_threads,
        lambda rel_path: shutil.copy2(
            source_folder / rel_path, target_folder / rel_path
        ),
        actions[Action.COPY_FILE],
    )

    logging.info(
        f"Finished syncing {source_folder} to {target_folder} in {time.time() - start_time:.2f} seconds"
    )
