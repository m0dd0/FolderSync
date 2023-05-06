from pathlib import Path
from typing import List, Callable
import hashlib
import logging
import concurrent
import os
import shutil
from collections import defaultdict
import time

from tqdm import tqdm


def metadata_hash(file_path: Path) -> str:
    """Hash files based on their modification stamp and file name and size.

    Args:
        file_path: path to the file

    Returns:
        "hash" of the file metadata
    """
    # NOTE: it makes no sense to hash on all attributes of path.stat() as the inode number and pther fields are not preserved when copying the file
    return (
        f"{file_path.name}_{os.path.getmtime(file_path)}_{os.path.getsize(file_path)}"
    )


def content_hash(
    file_path: Path, hash_func: Callable = hashlib.md5, block_size: int = 65536
) -> str:
    """Return the md5 hash of a file considering its content only.

    Args:
        file_path: path to the file
        hash_func: hash function to use, default is md5
        block_size: size of the block to read from the file at once, default is 65536

    Returns:
        md5 hash of the file content
    """
    hasher = hash_func()

    with open(file_path, "rb") as file:
        while True:
            data = file.read(block_size)
            if not data:
                break
            hasher.update(data)

    content_hash = hasher.hexdigest()

    return content_hash


def _delete_outdated_element(target_path: Path):
    if target_path.is_file():
        target_path.unlink()
        return "deleted_file"
    elif target_path.is_dir():
        shutil.rmtree(target_path)
        return "deleted_folder"


def _copy_new_element(source_path: Path, target_path: Path):
    if source_path.is_file():
        shutil.copy2(source_path, target_path)
        return "copied_file"
    elif source_path.is_dir():
        # TODO do own recursion for better tracking of progress, !!! accoutn for empty folders
        shutil.copytree(source_path, target_path)
        return "copied_folder"


def _update_existing_file(
    source_path: Path, target_path: Path, file_hash_func: Callable
):
    if file_hash_func(source_path) == file_hash_func(target_path):
        return "unchanged_file"
    target_path.unlink()
    shutil.copy2(source_path, target_path.parent)
    return "changed_file"


def _sync_dir(
    source_dir: Path,
    target_dir: Path,
    file_hash_func: Callable,
    executer: concurrent.futures.ThreadPoolExecutor,
    futures: List,
):
    target_names = [p.name for p in target_dir.iterdir()]
    source_names = [p.name for p in source_dir.iterdir()]

    # delete files and folders which are in tagret but not in source anymore
    for name in target_names:
        if name not in source_names:
            target_path = target_dir / name
            futures.append(executer.submit(_delete_outdated_element, target_path))

    # copy files and folders which are in source but not in target
    for name in source_names:
        source_path = source_dir / name
        target_path = target_dir / name

        if name not in target_names:
            futures.append(executer.submit(_copy_new_element, source_path, target_path))

        else:  # name exists in source and target
            if source_path.is_file():
                futures.append(
                    executer.submit(
                        _update_existing_file, source_path, target_path, file_hash_func
                    )
                )
            elif source_path.is_dir():
                _sync_dir(source_path, target_path, file_hash_func, executer, futures)


def sync_folders(
    source_folder: Path,
    target_folder: Path,
    n_thredas: int = 1,
    hash_func: Callable = metadata_hash,
):
    """Sync two folders recursively.
    All files and folders which are in target but not in source anymore are deleted.
    All files and folders which are in source but not in target are copied.
    All files which are in both source and target are updated if their hash is different.

    Args:
        source_folder: path to the source folder
        target_folder: path to the target folder
        n_thredas: number of threads to use, default is 1
        hash_func: function to use to hash files, default is metadata_hash
    """
    logging.info(f"Syncing {source_folder} to {target_folder}")

    start_time = time.time()
    stats = defaultdict(int)

    executer = concurrent.futures.ThreadPoolExecutor(max_workers=n_thredas)
    futures = []

    logging.info("Detecting changes...")
    _sync_dir(source_folder, target_folder, hash_func, executer, futures)

    logging.info("Applying changes...")
    with tqdm(total=len(futures)) as pbar:
        for future in concurrent.futures.as_completed(futures):
            pbar.update(1)
            result = future.result()
            stats[result] += 1

    executer.shutdown(wait=True)  # should be shutdown already but just to be sure

    logging.info(
        f"Finished syncing {source_folder} to {target_folder} in {time.time() - start_time:.2f} seconds"
    )
    for key, value in stats.items():
        logging.info(f"{key}: {value}")
