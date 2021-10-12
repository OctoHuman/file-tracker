"""Common utils"""

import re
import sys
import os
import types
from pathlib import Path
from typing import Generator

SQL_SAFE_CHARS_REGEX = "^[A-z0-9-]{1,100}$"

def assert_sql_safe_chars(string: str) -> None:
    """Raises an exception if given string contains SQL unsafe chars."""
    if not isinstance(string, str):
        raise TypeError("Argument isn't a string.")
    if not re.fullmatch(SQL_SAFE_CHARS_REGEX, string):
        raise ValueError(f"Argument has unsafe characters: ${string}")

def is_sqlite_db(path: Path) -> bool:
    """Checks if given file contains a SQLite database."""
    if not path.is_file():
        raise FileNotFoundError(f"Path given doesn't exist: {path}")

    # SQLite database header is 100 bytes long.
    if path.stat().st_size < 100:
        return False

    with path.open(mode="rb") as f:
        header = f.read(16)

    if header == b"SQLite format 3\0":
        return True
    else:
        return False

def get_fsid(path: Path) -> int:
    """Gets the filesystem ID of a given path."""
    if sys.platform == "linux":
        return os.statvfs(path).f_fsid
    elif sys.platform == "win32":
        return path.stat().st_dev
    else:
        raise NotImplementedError(f"Unsupported platform: {sys.platform}")

def dump_database(path: Path) -> None:
    """Dumps the contents of a `FileMetadataDb` into stdout as a table."""
    from file_metadata_db import FileMetadataDb
    path = Path(path)
    print("Path".ljust(80) + " | " + "Hash".ljust(64) + " | " + "Size".ljust(10) + " | " + "mtime".ljust(18) + " | " + "fs_id".ljust(10))
    with FileMetadataDb(path) as db:
        for file in db.get_all_files():
            # Path, hash, size, mtime, fs_id
            f_path = str(file.path).ljust(80)
            f_hash = file.hash.hex()
            f_size = str(file.size).ljust(10)
            f_mtime = str(file.mtime).ljust(18)
            f_fs_id = str(file.fs_id).ljust(10)
            print(f"{f_path} | {f_hash} | {f_size} | {f_mtime} | {f_fs_id}")
    print("Done.")

def walk_files(path: str | Path,
               error_handler: types.FunctionType
              ) -> Generator[str, None, None]:
    """
    Simplifies `os.walk` into a function that only yields file paths.

    Arguments:
        path:
          Path to walk recursively.
        error_handler:
          A function to call when there is an error iterating over the
          directories (such as a PermissionError). The given function is called
          with 1 argument, an exception representing the error that occurred.
    """
    if not callable(error_handler):
        raise TypeError("error_handler must be a function.")

    for root, dirs, files in os.walk(path, onerror=error_handler):
        root = Path(root)
        for file in files:
            yield root.joinpath(file)
