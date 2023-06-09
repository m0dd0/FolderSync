from pathlib import Path
from typing import List, Tuple, Set, Callable, Any, Dict
import logging
import concurrent.futures
import shutil
import time
import filecmp
import enum
from collections import defaultdict

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


def _sequential_execution(func: Callable, data: List[Any]) -> List[Any]:
    return [func(*d) for d in data]


def _chunk_list(l: List[Any], chunk_size: int) -> List[List[Any]]:
    return [l[i : i + chunk_size] for i in range(0, len(l), chunk_size)]


def _run_executer_with_progress(
    func: Callable,
    data: List[Tuple[Any]],
    n_threads: int,
    order: List[int] = None,
    datapoints_per_future: int = 1,
) -> List[List[Any]]:
    """Executes a function in parallel on all data and shows a progress bar which is
    updaed for each finished task.

    Args:
        n_threads (int): The number of threads to use
        func (Callable): The function to execute
        data (List[Tuple[Any]]): The data to pass to the function
        order (List[int], optional): The order in which the data should be processed. Defaults to None.
            It is ensured that data with lower order is processed before data with higher order.
            Data with the same order can be processed in parallel.
        datapoints_per_future (int, optional): The number of datapoints which are processed in one future/single thread.
    """

    if order is None:
        order = [0] * len(data)

    ordered_data = defaultdict(list)
    for idx, (val, ord) in enumerate(zip(data, order)):
        ordered_data[ord].append((idx, val))

    final_results = [None] * len(data)

    with tqdm(total=len(data)) as pbar:
        for ord, indexed_data in sorted(ordered_data.items()):
            indices, arguments = zip(*indexed_data)
            argument_chunks = _chunk_list(arguments, datapoints_per_future)

            executer = concurrent.futures.ThreadPoolExecutor(max_workers=n_threads)
            chunk_futures = [
                executer.submit(_sequential_execution, func, arg_chunk)
                for arg_chunk in argument_chunks
            ]

            for fut in concurrent.futures.as_completed(chunk_futures):
                pbar.update(len(fut.result()))

            ord_results = [r for fut in chunk_futures for r in fut.result()]
            executer.shutdown(wait=True)

            for idx, result in zip(indices, ord_results):
                final_results[idx] = result

    return final_results


def _handle_invlid_types(
    source_paths: Set[Path], target_paths: Set[Path], quiet: bool
) -> Tuple[Set[Path], Set[Path]]:
    """Checks if the paths are valid (i.e. if they are files or directories) and
    asks the user if he wants to continue if there are invalid paths.
    Invalid paths are deleted from the target folder.

    Args:
        source_paths (Set[Path]): The paths in the source folder
        target_paths (Set[Path]): The paths in the target folder
        quiet (bool): If True, the user wont be asked if he wants to continue.

    Returns:
        Tuple[Set[Path], Set[Path]]: The invalid paths in the source and target folder
    """
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
        if not quiet:
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
    """Determines the change of a file in the target folder for a given relative path.

    Args:
        rel_path (Path): The relative path of the file
        source_folder (Path): The source folder
        target_folder (Path): The target folder
        shallow_comparison (bool): Whether to use a shallow comparison for files
        in_source (bool): Whether the file is in the source folder
        in_target (bool): Whether the file is in the target folder

    Returns:
        Change: The type of change of the file
    """
    source_path = source_folder / rel_path
    target_path = target_folder / rel_path

    if not in_source and in_target:
        if target_path.is_file():
            return Change.REMOVED_FILE
        elif target_path.is_dir():
            return Change.REMOVED_FOLDER
        else:
            assert False, "There shouldnt be any paths which arent files or folder"

    if in_source and not in_target:
        if source_path.is_file():
            return Change.NEW_FILE
        elif source_path.is_dir():
            return Change.NEW_FOLDER
        else:
            assert False, "There shouldnt be any paths which arent files or folder"

    if in_source and in_target:
        if source_path.is_file() and target_path.is_file():
            if filecmp.cmp(source_path, target_path, shallow_comparison):
                return Change.UNCHANGED_FILE
            else:
                return Change.CHANGED_FILE
        elif source_path.is_file() and target_path.is_dir():
            return Change.CHANGED_FOLDER2FILE
        elif source_path.is_dir() and target_path.is_file():
            return Change.CHANGED_FILE2FOLDER
        elif source_path.is_dir() and target_path.is_dir():
            return Change.UNCHANGED_FOLDER
        else:
            assert False, "There shouldnt be any paths which arent files or folder"


def _get_changes(
    n_threads: int,
    operations_per_thread: int,
    source_folder: Path,
    target_folder: Path,
    shallow_comparison: bool,
    source_paths_rel: Set[Path],
    target_paths_rel: Set[Path],
) -> Dict[Change, Set[Path]]:
    """Determines the changes of all files in the target folder and returns them as a dictionary mapped to the change type.
    Execution is parallelized.

    Args:
        n_threads (int): The number of threads to use
        source_folder (Path): The source folder
        target_folder (Path): The target folder
        shallow_comparison (bool): Whether to use a shallow comparison for files
        source_paths_rel (Set[Path]): The relative paths of the files in the source folder
        target_paths_rel (Set[Path]): The relative paths of the files in the target folder

    Returns:
        Dict[Change, Set[Path]]: A dictionary mapping the change type to the relative paths of the files with that change
    """
    all_paths_rel = list(source_paths_rel | target_paths_rel)
    change_results = _run_executer_with_progress(
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
        n_threads,
        datapoints_per_future=operations_per_thread,
    )
    changes = {c: set() for c in Change}
    for change_type, rel_path in zip(change_results, all_paths_rel):
        changes[change_type].add(rel_path)

    return changes


def _infer_actions(changes: Dict[Change, Set[Path]]) -> Dict[Action, Set[Path]]:
    """Infers the actions to be taken for each change type.

    Args:
        changes (Dict[Change, Set[Path]]): A dictionary mapping the change type to the relative paths of the files with that change

    Returns:
        Dict[Action, Set[Path]]: A dictionary mapping the action type to the relative paths of the files with that action
    """
    actions = {e: set() for e in Action}
    for change_type, paths in changes.items():
        if change_type == Change.REMOVED_FILE:
            actions[Action.DELETE_FILE].update(paths)
        elif change_type == Change.REMOVED_FOLDER:
            actions[Action.DELETE_FOLDER].update(paths)
        elif change_type == Change.NEW_FILE:
            actions[Action.COPY_FILE].update(paths)
        elif change_type == Change.NEW_FOLDER:
            actions[Action.CREATE_FOLDER].update(paths)
        elif change_type == Change.CHANGED_FILE:
            actions[Action.DELETE_FILE].update(paths)
            actions[Action.COPY_FILE].update(paths)
        elif change_type == Change.CHANGED_FILE2FOLDER:
            actions[Action.DELETE_FILE].update(paths)
            actions[Action.CREATE_FOLDER].update(paths)
        elif change_type == Change.CHANGED_FOLDER2FILE:
            actions[Action.DELETE_FOLDER].update(paths)
            actions[Action.COPY_FILE].update(paths)

    return actions


def _capped_path_list(paths: Set[Path], max_lines: int) -> str:
    """Returns a string representation of the paths, capped at max_lines.

    Args:
        paths (Set[Path]): The paths
        max_lines (int): The maximum number of lines

    Returns:
        str: The string representation of the paths
    """
    paths = list(paths)
    if max_lines < 0:
        max_lines = len(paths)
    return "\n".join([str(p) for p in paths[:max_lines]]) + (
        "\n..." if len(paths) > max_lines else ""
    )


def _log_actions(
    actions: Dict[Change, Set[Path]],
    changes: Dict[Action, Set[Path]],
    max_lines: int,
    quiet: bool,
):
    """Logs the actions and changes to be taken and asks the user for confirmation.

    Args:
        actions (Dict[Change, Set[Path]]): A dictionary mapping the action type to the relative paths of the files with that action
        changes (Dict[Action, Set[Path]]): A dictionary mapping the change type to the relative paths of the files with that change
        max_lines (int): The maximum number of lines to print
        quiet (bool): Whether to skip the confirmation.
    """
    logging.info(
        f"{len(changes[Change.UNCHANGED_FILE])} files are unchanged."
        + f"\n{_capped_path_list(changes[Change.UNCHANGED_FILE], max_lines)}"
    )

    logging.info(
        f"{len(changes[Change.CHANGED_FILE])} files are changed."
        + f"\n{_capped_path_list(changes[Change.CHANGED_FILE], max_lines)}"
    )

    new_files = changes[Change.NEW_FILE] | changes[Change.CHANGED_FOLDER2FILE]
    logging.info(
        f"{len(new_files)} files are new."
        + f"\n{_capped_path_list(new_files, max_lines)}"
    )

    new_folders = changes[Change.NEW_FOLDER] | changes[Change.CHANGED_FILE2FOLDER]
    logging.info(
        f"{len(new_folders)} folders are new."
        + f"\n{_capped_path_list(new_folders, max_lines)}"
    )

    removed_files = changes[Change.REMOVED_FILE] | changes[Change.CHANGED_FILE2FOLDER]
    logging.info(
        f"{len(removed_files)} files are removed."
        + f"\n{_capped_path_list(removed_files, max_lines)}"
    )

    removed_folders = (
        changes[Change.REMOVED_FOLDER] | changes[Change.CHANGED_FOLDER2FILE]
    )
    logging.info(
        f"{len(removed_folders)} folders are removed."
        + f"\n{_capped_path_list(removed_folders, max_lines)}"
    )

    logging.info("Do you want to continue? (y/n)")
    if not quiet:
        if input("y/n: ") != "y":
            logging.info("Aborting...")
            exit()


def sync_folders(
    source_folder: Path,
    target_folder: Path,
    n_threads: int = 100,
    operations_per_thread: int = 10,
    shallow_comparison: bool = True,
    max_logged_paths: int = -1,
    quiet: bool = False,
):
    """Syncs two folders. This means the target folder will be made identical to the source folder.
    Identical files will be untouched, files that are only in the source folder will be copied to the target folder,
    files that are only in the target folder will be deleted, and files that are in both folders but have different
    contents will be overwritten.

    Args:
        source_folder (Path): The path to the source folder
        target_folder (Path): The path to the target folder
        n_threads (int, optional): The number of threads to use for the comparison. Defaults to 1.
        operations_per_thread (int, optional): The number of operations to perform per thread. Defaults to 10.
        shallow_comparison (bool, optional): Whether to only compare the file sizes and modification times. Defaults to True.
        max_logged_paths (bool, optional): The maximum number of paths to log. If negative, all paths will be logged. Defaults to -1.
        quiet (bool, optional): Whether to ask the user for confirmation. Defaults to False.
    """
    logging.info(f"Syncing {source_folder} to {target_folder}")
    if not source_folder.exists():
        logging.error(f"{source_folder} does not exist.")
        exit()
    if not target_folder.exists():
        logging.error(f"{target_folder} does not exist.")
        exit()
    start_time = time.time()

    logging.info("Detecting paths...")
    source_paths = set(source_folder.rglob("*"))
    target_paths = set(target_folder.rglob("*"))

    logging.info("Checking for invalid types")
    invalid_source_paths, invalid_target_paths = _handle_invlid_types(
        source_paths, target_paths, quiet
    )
    source_paths = source_paths - invalid_source_paths
    target_paths = target_paths - invalid_target_paths

    logging.info("Making relative paths...")
    source_paths_rel = {p.relative_to(source_folder) for p in source_folder.rglob("*")}
    target_paths_rel = {p.relative_to(target_folder) for p in target_folder.rglob("*")}

    logging.info("Determining changes...")
    changes: Dict[Change, Set[Path]] = _get_changes(
        n_threads,
        operations_per_thread,
        source_folder,
        target_folder,
        shallow_comparison,
        source_paths_rel,
        target_paths_rel,
    )

    logging.info("Inferring actions...")
    actions: Dict[Action, Set[Path]] = _infer_actions(changes)

    logging.info("The following actions will be applied on the target folder:")
    _log_actions(actions, changes, max_logged_paths, quiet)

    logging.info("Applying changes...")

    logging.info("Deleting files...")
    _run_executer_with_progress(
        lambda rel_path: (target_folder / rel_path).unlink(),
        [(p,) for p in actions[Action.DELETE_FILE]],
        n_threads,
        datapoints_per_future=operations_per_thread,
    )

    logging.info("Deleting (now empty) folders...")
    _run_executer_with_progress(
        lambda rel_path: (target_folder / rel_path).rmdir(),
        [(p,) for p in actions[Action.DELETE_FOLDER]],
        n_threads,
        order=[-len(p.parts) for p in actions[Action.DELETE_FOLDER]],
        datapoints_per_future=operations_per_thread,
    )

    logging.info("Creating folders...")
    _run_executer_with_progress(
        lambda rel_path: (target_folder / rel_path).mkdir(parents=True, exist_ok=True),
        [(a,) for a in actions[Action.CREATE_FOLDER]],
        n_threads,
        datapoints_per_future=operations_per_thread,
    )

    logging.info("Copying files...")
    _run_executer_with_progress(
        lambda rel_path: shutil.copy2(
            source_folder / rel_path, target_folder / rel_path
        ),
        [(a,) for a in actions[Action.COPY_FILE]],
        n_threads,
        datapoints_per_future=operations_per_thread,
    )

    logging.info(
        f"Finished syncing {source_folder} to {target_folder} in {time.time() - start_time:.2f} seconds"
    )
