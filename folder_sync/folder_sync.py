from pathlib import Path
from typing import Dict, List, Set, Callable
import hashlib
from fnmatch import fnmatch
import logging
from concurrent.futures import ThreadPoolExecutor
import os
import abc
import shutil
from collections import defaultdict

from tqdm import tqdm


class FileHasher(abc.ABC):
    @abc.abstractmethod
    def __call__(self, file_path: Path) -> str:
        """Abstract method for defining function which calculate some kind of hash
        for the whole file.

        Args:
            file_path (Path): The path to the file for which the hash should be generated.

        Returns:
            str: The hash for the file.
        """
        raise NotImplementedError


class MetadataHasher(FileHasher):
    def __init__(self) -> None:
        """Hash files based on their modification stamp and file name and size."""
        pass

    def __call__(self, file_path: Path) -> str:
        return f"{file_path.name}_{os.path.getmtime(file_path)}_{os.path.getsize(file_path)}"


class ContentHasher(FileHasher):
    def __init__(
        self, hash_func: Callable = hashlib.md5, block_size: int = 65536
    ) -> None:
        """Hash files based on their content.

        Args:
            hash_func (Callable, optional): The hash function to use. Defaults to hashlib.md5.
            block_size (int, optional): The block size to use when reading the file. Defaults to 65536.
        """
        self.block_size = block_size
        self.hash_func = hash_func

    def __call__(self, file_path: Path) -> str:
        """Return the md5 hash of a file."""
        hasher = self.hash_func()

        with open(file_path, "rb") as file:
            while True:
                data = file.read(self.block_size)
                if not data:
                    break
                hasher.update(data)

        content_hash = hasher.hexdigest()

        return content_hash


class Syncer:
    def __init__(
        self,
        source_folder: Path,
        target_folder: Path,
        file_hasher: FileHasher = None,
        # n_threads: int = 1,
    ) -> None:
        """Sync two folders. The folder are synced so that the target folder
        is a exact copy of the source folder. Only files which are different
        are copied.

        Args:
            source_folder (Path): The source folder.
            target_folder (Path): The target folder.
            file_hasher (FileHasher, optional): The file hasher to use. Defaults to a MetadataHasher with md5 as hash function.
            n_threads (int, optional): The number of threads to use for calculating the hashes and filtering the files.
                Defaults to 1.
        """
        self.source_folder = source_folder
        self.target_dir = target_folder
        self.file_hasher = file_hasher or MetadataHasher()
        self.stats = defaultdict(int)
        # self.n_threads = n_threads

    def _sync_dir(self, source_dir: Path, target_dir: Path):
        target_names = [p.name for p in target_dir.iterdir()]
        source_names = [p.name for p in source_dir.iterdir()]

        def change_file(source_path, target_path):
            source_path.unlink()
            shutil.copy2(source_path, target_path.parent)

        # delete files and folders which are in tagret but not in source anymore
        for name in target_names:
            if name not in source_names:
                target_path = target_dir / name
                if target_path.is_file():
                    target_path.unlink()
                    self.stats["deleted_files"] += 1
                elif target_path.is_dir():
                    shutil.rmtree(target_path)
                    self.stats["deleted_folders"] += 1

        # copy files and folders which are in source but not in target
        for name in source_names:
            source_path = source_dir / name
            target_path = target_dir / name

            if name not in target_names:
                if source_path.is_file():
                    # shutil.copy2(source_path, target_path)
                    self.executer.submit(shutil.copy2, source_path, target_path)
                    self.stats["copied_files"] += 1
                elif source_path.is_dir():
                    # shutil.copytree(source_path, target_path)
                    self.executer.submit(shutil.copytree, source_path, target_path)
                    self.stats["copied_folders"] += 1

            else:  # name exists in source and target
                if source_path.is_file():
                    if self.file_hasher(source_path) != self.file_hasher(target_path):
                        # target_path.unlink()
                        # shutil.copy2(source_path, target_path.parent)
                        self.executer.submit(change_file, source_path, target_path)
                        self.stats["changed_files"] += 1
                    else:
                        self.stats["unchanged_files"] += 1
                elif source_path.is_dir():
                    self._sync_dir(source_path, target_path)


# NOTE: this hashing method is not usable as the metadata is changed when copying the file (only the modification time is kept)
# as a result the hash for the files in the target will always be different from the hash in the source
# class MetadataHasher(FileHasher):
#     def __init__(
#         self,
#         hash_func: Callable = hashlib.md5,
#         attributes: Tuple[str] = (
#             "st_mode",
#             "st_ino",
#             "st_dev",
#             "st_nlink",
#             "st_uid",
#             "st_gid",
#             "st_size",
#             "st_atime",
#             "st_mtime",
#             "st_ctime",
#             "st_atime_ns",
#             "st_mtime_ns",
#             "st_ctime_ns",
#         ),
#     ) -> None:
#         """Hash files based on their metadata. This especially accounts for the modification time
#         and the file id.

#         Args:
#             hash_func (Callable, optional): The hash function to apply on the metadata. Defaults to hashlib.md5.
#             attributes (Tuple[str], optional): The attributes to use for the hash. Defaults to all available attributes.
#         """
#         self.hash_func = hash_func
#         self.attributes = attributes

#     def __call__(self, file_path: Path) -> str:
#         str_repr = ""
#         for attr in self.attributes:
#             str_repr += str(getattr(file_path.stat(), attr, ""))

#         metadata_hash = self.hash_func(str_repr.encode()).hexdigest()

#         return metadata_hash
