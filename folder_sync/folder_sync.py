from pathlib import Path
from typing import Dict, List, Set, Callable, Tuple
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


class ModificationStampHasher(FileHasher):
    def __init__(self) -> None:
        """Hash files based on their modification stamp and file name."""
        # self.base_path = base_path
        pass

    def __call__(self, file_path: Path) -> str:
        # rel_path = file_path.relative_to(self.base_path)
        # return f"{rel_path}_{os.path.getmtime(file_path)}"
        return f"{file_path.name}_{os.path.getmtime(file_path)}"


class MetadataHasher(FileHasher):
    def __init__(
        self,
        hash_func: Callable = hashlib.md5,
        attributes: Tuple[str] = (
            "st_mode",
            "st_ino",
            "st_dev",
            "st_nlink",
            "st_uid",
            "st_gid",
            "st_size",
            "st_atime",
            "st_mtime",
            "st_ctime",
            "st_atime_ns",
            "st_mtime_ns",
            "st_ctime_ns",
        ),
    ) -> None:
        """Hash files based on their metadata. This especially accounts for the modification time
        and the file id.

        Args:
            hash_func (Callable, optional): The hash function to apply on the metadata. Defaults to hashlib.md5.
            attributes (Tuple[str], optional): The attributes to use for the hash. Defaults to all available attributes.
        """
        self.hash_func = hash_func
        self.attributes = attributes

    def __call__(self, file_path: Path) -> str:
        str_repr = ""
        for attr in self.attributes:
            str_repr += str(getattr(file_path.stat(), attr, ""))

        metadata_hash = self.hash_func(str_repr.encode()).hexdigest()

        return metadata_hash


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
        excluded_paths: List[Path] = None,
        file_hasher: FileHasher = None,
        sync_method: str = "path",
        n_threads: int = 1,
    ) -> None:
        """Sync two folders. The folder are synced so that the target folder
        is a exact copy of the source folder. Only files which are different
        are copied.

        Args:
            source_folder (Path): The source folder.
            target_folder (Path): The target folder.
            excluded_paths (List[Path], optional): A list of paths to exclude from the sync. Defaults to None.
            file_hasher (FileHasher, optional): The file hasher to use. Defaults to a MetadataHasher with md5 as hash function.
            sync_method (str, optional): The sync method to use. Options are "path" and "hash".
                "path" syncs files based on their path. So a file must have the same path in the source
                and target folder and have the same hash in order to be not copied (or deleted).
                "hash" syncs files based on their hash. So if there are files in the target which have
                the same hash but a different path they will be moved to the correct path on the target instead
                of being copied from the source.
                Defaults to "path".
            n_threads (int, optional): The number of threads to use for calculating the hashes and filtering the files.
                Defaults to 1.
        """
        self.source_folder = source_folder
        self.target_folder = target_folder
        self.excluded_paths = excluded_paths or []
        self.file_hasher = file_hasher or MetadataHasher()
        self.n_threads = n_threads

        self.sync_method = None
        if sync_method == "path":
            self.sync_method = self._sync_by_path
        elif sync_method == "hash":
            self.sync_method = self._sync_by_hash
        else:
            raise ValueError(f"Unknown sync method {sync_method}")

    def _validate_file(self, file_path: Path) -> bool:
        """Halper function which return True if file is valid, False otherwise.
        A file is valid if it is a file and not in the excluded paths.

        Args:
            file_path (Path): The file to check.

        Returns:
            bool: True if file is valid, False otherwise.
        """
        try:
            if file_path.is_file() and not any(
                fnmatch(str(file_path), str(excluded_path))
                for excluded_path in self.excluded_paths
            ):
                return True
        except OSError:
            logging.warning(f"Could not read {file_path}")

    def _all_files(self, folder_path: Path) -> List[Path]:
        """Helper function which return a list of all valid files in the given folder.
        Uses multithreading.

        Args:
            folder_path (Path): The folder to search in.

        Returns:
            List[Path]: A list of all valid files in the folder.

        """
        paths = list(folder_path.rglob("*"))

        with ThreadPoolExecutor(max_workers=self.n_threads) as executor:
            with tqdm(total=len(paths)) as pbar:
                futures = []
                for p in paths:
                    future = executor.submit(self._validate_file, p)
                    future.add_done_callback(lambda _: pbar.update())
                    futures.append(future)

                paths = [p for p, future in zip(paths, futures) if future.result()]

        return paths

    def _path_hash_mapping(
        self, file_paths: List[Path], file_hash_func: Callable
    ) -> Dict[Path, str]:
        """Helper function which returns a mapping of file paths to their hashes.
        Uses multithreading.

        Args:
            file_paths (List[Path]): The file paths to hash.
            file_hash_func (Callable): The hash function to use.

        Returns:
            Dict[Path, str]: A mapping of file paths to their hashes.
        """
        with ThreadPoolExecutor(max_workers=self.n_threads) as executor:
            with tqdm(total=len(file_paths)) as pbar:
                futures = []
                for file_path in file_paths:
                    future = executor.submit(file_hash_func, file_path)
                    future.add_done_callback(lambda _: pbar.update())
                    futures.append(future)

                file_hashes = [future.result() for future in futures]

        result = dict(zip(file_paths, file_hashes))

        return result

    def _relative_path_hash_mapping(
        self, mapping: Dict[Path, str], base_path: Path
    ) -> Dict[Path, str]:
        """Helper function which returns a mapping of relative file paths to their hashes.
        The relative paths are relative to the given base path.
        This makes them identical for the source and target folder.

        Args:
            mapping (Dict[Path, str]): The mapping of file paths to their hashes.
            base_path (Path): The base path to relativize the paths.

        Returns:
            Dict[Path, str]: A mapping of relative file paths to their hashes.
        """
        result = {}
        for path, hash in mapping.items():
            rel_path = path.relative_to(base_path)
            result[rel_path] = hash

        return result

    def _sync_by_path(
        self,
        source_path_hash_mapping: Dict[Path, str],
        target_path_hash_mapping: Dict[Path, str],
    ) -> None:
        """
        Syncs the target folder with the source folder by comparing the relative paths of the files.
        Files which have the same path in the source and target folder and have the same hash
        are not copied (or deleted). Files which have the same path but a different hash are
        copied from the source folder to the target folder.
        Files which are in the target folder but not in the source folder are deleted.
        If a path is not in the target folder it is created and the file is copied from the source folder.
        However this means that renaming a directory in the source folder will result in the directory
        being deleted in the target folder and a new directory being created/copied in the target folder.
        Logs it progress to the console.

        Args:
            source_path_hash_mapping (Dict[Path, str]): A mapping of relative file paths to their hashes in the source folder.
            target_path_hash_mapping (Dict[Path, str]): A mapping of relative file paths to their hashes in the target folder.
        """
        statistics = {"unchanged": 0, "changed": 0, "new": 0, "removed": 0}

        logging.info("Deleting files in target that are not in source...")
        for rel_path, hash in tqdm(target_path_hash_mapping.items()):
            if rel_path not in source_path_hash_mapping.keys():
                (self.target_folder / rel_path).unlink()
                statistics["removed"] += 1

        logging.info("Copying files from source to target...")
        for rel_path, hash in tqdm(source_path_hash_mapping.items()):
            if rel_path in target_path_hash_mapping.keys():
                if hash != target_path_hash_mapping[rel_path]:
                    (self.target_folder / rel_path).unlink()
                    shutil.copy2(
                        self.source_folder / rel_path,
                        (self.target_folder / rel_path).parent,
                    )
                    statistics["changed"] += 1
                else:
                    statistics["unchanged"] += 1

            else:
                (self.target_folder / rel_path).parent.mkdir(
                    parents=True, exist_ok=True
                )
                shutil.copy2(
                    self.source_folder / rel_path,
                    (self.target_folder / rel_path).parent,
                )
                statistics["new"] += 1

        logging.info(f"Synced {sum(statistics.values())} files:")
        for key, value in statistics.items():
            logging.info(f"{key}: {value}")

    def _invert_mapping(self, mapping: Dict[Path, str]) -> Dict[str, Set[Path]]:
        """Helper function which inverts a mapping of file paths to their hashes.
        The result is a mapping of hashes to a set of file paths
        (as files with identical hashes can be present mutliple times).

        Args:
            mapping (Dict[Path, str]): The mapping of file paths to their hashes.

        Returns:
            Dict[str, Set[Path]]: A mapping of hashes to a set of file paths.
        """
        result = defaultdict(set)
        for path, hash in mapping.items():
            result[hash].add(path)

        return result

    def _sync_by_hash(
        self,
        source_path_hash_mapping: Dict[Path, str],
        target_path_hash_mapping: Dict[Path, str],
    ) -> None:
        """
        Syncs the target folder with the source folder by comparing the hashes of the files.
        Files in the target folder whose hash are not in the source folder are deleted.
        Files in the source folder whose hash are not in the target folder are copied to the target folder.
        Files in the source folder whose hash are in the target folder but have a different path
            are moved within the target folder.
        Will be faster when large subtrees are moved or renamed but the file contents are the same.
        MAke sure to use a hash functions which accounts for the metadata of a file as otherwise
        the metadata of files which are moved on the target are not in sync anymore with the source folder.

        Args:
            source_path_hash_mapping (Dict[Path, str]): A mapping of relative file paths to their hashes in the source folder.
            target_path_hash_mapping (Dict[Path, str]): A mapping of relative file paths to their hashes in the target folder.
        """
        source_hash_paths_mapping = self._invert_mapping(source_path_hash_mapping)
        target_hash_paths_mapping = self._invert_mapping(target_path_hash_mapping)

        statistics = {"unchanged": 0, "changed": 0, "new": 0, "removed": 0, "moved": 0}

        logging.info("Deleting files in target that are not in source...")
        for hash, paths in tqdm(target_hash_paths_mapping.items()):
            if hash not in source_hash_paths_mapping.keys():
                for path in paths:
                    path.unlink()
                    statistics["removed"] += 1

        logging.info("Copying files from source to target...")
        for hash, paths in tqdm(source_hash_paths_mapping.items()):
            if hash not in target_hash_paths_mapping.keys():
                for path in paths:
                    (self.target_folder / path).parent.mkdir(
                        parents=True, exist_ok=True
                    )
                    shutil.copy2(
                        self.source_folder / path, (self.target_folder / path).parent
                    )
                    statistics["new"] += 1
                continue

            if paths == target_hash_paths_mapping[hash]:
                statistics["unchanged"] += len(paths)
                continue

            new_paths = paths - target_hash_paths_mapping[hash]
            old_paths = target_hash_paths_mapping[hash] - paths
            for path in new_paths:
                (self.target_folder / path).parent.mkdir(parents=True, exist_ok=True)
                if len(old_paths) > 0:
                    shutil.move(
                        self.source_folder / old_paths.pop(),
                        self.target_folder / path,
                    )
                    statistics["moved"] += 1
                else:
                    shutil.copy2(
                        self.source_folder / path,
                        (self.target_folder / path).parent,
                    )
                    statistics["new"] += 1

        logging.info(f"Synced {sum(statistics.values())} files:")
        for key, value in statistics.items():
            logging.info(f"{key}: {value}")

    def __call__(self) -> None:
        """Executes all steps of the sync process and logs the progress."""
        logging.info(f"Syncing {self.source_folder} to {self.target_folder}")

        logging.info("Getting all valid files in source folder...")
        source_file_paths = self._all_files(self.source_folder)
        logging.info("Hashing all valid files in source folder...")
        source_path_hash_mapping = self._path_hash_mapping(
            source_file_paths, self.file_hasher
        )
        logging.info("Making paths relative to source folder...")
        source_path_map_hashing = self._relative_path_hash_mapping(
            source_path_hash_mapping, self.source_folder
        )

        logging.info("Getting all valid files in target folder...")
        target_file_paths = self._all_files(self.target_folder)
        logging.info("Hashing all valid files in target folder...")
        target_path_hash_mapping = self._path_hash_mapping(
            target_file_paths, self.file_hasher
        )
        logging.info("Making paths relative to target folder...")
        target_path_hash_mapping = self._relative_path_hash_mapping(
            target_path_hash_mapping, self.target_folder
        )

        logging.info("Syncing files...")
        self.sync_method(source_path_map_hashing, target_path_hash_mapping)
