from pathlib import Path
from typing import List, Tuple, Set, Callable, Any, Dict, Union
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


def _run_executer_with_progress(
    n_threads: int,
    func: Callable,
    data: Union[List[Tuple], List[List[Tuple]]],
    batched: bool = False,
) -> List[List[Any]]:
    """Executes a function in parallel on all data and shows a progress bar which is
    updaed for each finished task.

    Args:
        n_threads (int): The number of threads to use
        func (Callable): The function to execute
        data (Union[List[Tuple], List[List[Tuple]]]): The data to pass to the function.
            If batched is False, this should be a list of tuples.
            Each elemnt of the tuple will be passed as an argument to the function.
            If batched is True, this should be a list of lists of tuples.
            The outer list represents the batches and the inner list represents the
            arguments for each batch.
        batched (bool, optional): Whether the data is batched or not. Defaults to False.
            If the data is batches the data in the first batch will be processed first and so on.

    Returns:
        List[List[Any]]: The results of the function calls.
            If batched is False, this will be a list of the results of the function calls.
            If batched is True, this will be a list of lists of the results of the function calls.
            The outer list represents the batches and the inner list represents the results of each batch.
    """

    if not batched:
        data_batches = [data]
    else:
        data_batches = data

    results = []
    with tqdm(total=sum(len(d) for d in data_batches)) as pbar:
        for data in data_batches:
            executer = concurrent.futures.ThreadPoolExecutor(max_workers=n_threads)
            futures = [executer.submit(func, *d) for d in data]

            for _ in concurrent.futures.as_completed(futures):
                pbar.update(1)

                executer.shutdown(wait=True)

            results.append([f.result() for f in futures])

    if not batched:
        return results[0]

    return results


def _handle_invlid_types(
    source_paths: Set[Path], target_paths: Set[Path], ask: bool
) -> Tuple[Set[Path], Set[Path]]:
    """Checks if the paths are valid (i.e. if they are files or directories) and
    asks the user if he wants to continue if there are invalid paths.
    Invalid paths are deleted from the target folder.

    Args:
        source_paths (Set[Path]): The paths in the source folder
        target_paths (Set[Path]): The paths in the target folder
        ask (bool): Whether to ask the user if he wants to continue if there are invalid paths

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
        if ask:
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


def _log_actions(
    actions: Dict[Change, Set[Path]],
    changes: Dict[Action, Set[Path]],
    verbose: bool,
    ask: bool,
):
    """Logs the actions and changes to be taken and asks the user for confirmation.

    Args:
        actions (Dict[Change, Set[Path]]): A dictionary mapping the action type to the relative paths of the files with that action
        changes (Dict[Action, Set[Path]]): A dictionary mapping the change type to the relative paths of the files with that change
        verbose (bool): Whether to log the paths of the files with each action
        ask (bool): Whether to ask the user for confirmation
    """
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
    if ask:
        if input("y/n: ") != "y":
            logging.info("Aborting...")
            exit()


def _get_paths_in_levelbatches(paths: Set[Path]) -> List[Set[Tuple[Path]]]:
    """Groups the paths by their depth and returns them as a list of batches.
    The batches are sorted by the depth of the paths in them, with the deepest paths first.

    Args:
        paths (Set[Path]): The paths to group

    Returns:
        List[Set[Tuple[Path]]]: A list of batches of paths
    """
    paths_by_depth = defaultdict(list)
    for p in paths:
        paths_by_depth[len(p.parts)].append(p)
    path_batches = []
    for level in sorted(paths_by_depth.keys(), reverse=True):
        path_batches.append([(p,) for p in paths_by_depth[level]])

    return path_batches


def sync_folders(
    source_folder: Path,
    target_folder: Path,
    n_threads: int = 1,
    shallow_comparison: bool = True,
    verbose_logging: bool = False,
    ask: bool = True,
):
    """Syncs two folders. This means the target folder will be made identical to the source folder.
    Identical files will be untouched, files that are only in the source folder will be copied to the target folder,
    files that are only in the target folder will be deleted, and files that are in both folders but have different
    contents will be overwritten.

    Args:
        source_folder (Path): The path to the source folder
        target_folder (Path): The path to the target folder
        n_threads (int, optional): The number of threads to use for the comparison. Defaults to 1.
        shallow_comparison (bool, optional): Whether to only compare the file sizes and modification times. Defaults to True.
        verbose_logging (bool, optional): Whether to log the paths of the files with each action. Defaults to False.
        ask (bool, optional): Whether to ask the user for confirmation. Defaults to True.
    """
    logging.basicConfig(level=logging.INFO)

    logging.info(f"Syncing {source_folder} to {target_folder}")
    print(f"Syncing {source_folder} to {target_folder}")
    start_time = time.time()

    logging.info("Detecting paths...")
    source_paths = set(source_folder.rglob("*"))
    target_paths = set(target_folder.rglob("*"))

    logging.info("Checking for invalid types")
    invalid_source_paths, invalid_target_paths = _handle_invlid_types(
        source_paths, target_paths, ask
    )
    source_paths = source_paths - invalid_source_paths
    target_paths = target_paths - invalid_target_paths

    logging.info("Making relative paths...")
    source_paths_rel = {p.relative_to(source_folder) for p in source_folder.rglob("*")}
    target_paths_rel = {p.relative_to(target_folder) for p in target_folder.rglob("*")}

    logging.info("Determining changes...")
    changes: Dict[Change, Set[Path]] = _get_changes(
        n_threads,
        source_folder,
        target_folder,
        shallow_comparison,
        source_paths_rel,
        target_paths_rel,
    )

    logging.info("Inferring actions...")
    actions: Dict[Action, Set[Path]] = _infer_actions(changes)

    logging.info("The following actions will be applied on the target folder:")
    _log_actions(actions, changes, verbose_logging, ask)

    logging.info("Applying changes...")

    logging.info("Deleting files...")
    _run_executer_with_progress(
        n_threads,
        lambda rel_path: (target_folder / rel_path).unlink(),
        [(p,) for p in actions[Action.DELETE_FILE]],
    )

    logging.info("Deleting (now empty) folders...")
    _run_executer_with_progress(
        n_threads,
        lambda rel_path: (target_folder / rel_path).rmdir(),
        _get_paths_in_levelbatches(actions[Action.DELETE_FOLDER]),
        batched=True,
    )

    logging.info("Creating folders...")
    _run_executer_with_progress(
        n_threads,
        lambda rel_path: (target_folder / rel_path).mkdir(parents=True, exist_ok=True),
        [(a,) for a in actions[Action.CREATE_FOLDER]],
    )

    logging.info("Copying files...")
    _run_executer_with_progress(
        n_threads,
        lambda rel_path: shutil.copy2(
            source_folder / rel_path, target_folder / rel_path
        ),
        [(a,) for a in actions[Action.COPY_FILE]],
    )

    logging.info(
        f"Finished syncing {source_folder} to {target_folder} in {time.time() - start_time:.2f} seconds"
    )
