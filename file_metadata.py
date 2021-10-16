"""
Calculates and caches metadata from files.

This module contains classes that allow for consistent and performant access
of a file's metadata.
"""

from pathlib import Path
import hashlib
from typing import Optional
import utils

CHUNK_SIZE = 64000000 #64MB

class FileMetadata:
    """
    Abstracts a file into an object containing its metadata.

    Given a path, this class stores metadata of the file, and caches it,
    allowing for higher performance when the metadata has to be accessed many
    times.

    Attributes:
        hash:
          A `bytes` object representing the SHA256 hash of the file.
        path:
          A `path` object of the files location.
        size:
          File size in bytes.
        mtime:
          The files last mtime in nanoseconds.
        fs_id:
          The filesystem ID that the file is stored on. This ID is unique per
          filesystem.
    """
    def __init__(self, path: Path) -> None:
        """
        Use given `Path` object to init FileMetadata object.

        Args:
            path:
              `Path` object representing the file to obtain metadata from.

        Raises:
            FileNotFoundError:
              There is no file with the given `path`.
            TypeError:
              The `path` given points to a symlink. This is unsupported.
        """
        if not isinstance(path, Path):
            raise TypeError("Path argument isn't a pathlib.Path object.")
        if not path.is_file():
            raise FileNotFoundError(f"Given path '{path}' is not a file.")
        if path.is_symlink():
            raise TypeError(f"Given path points to a symlink, which is unsupported: {path}")

        self._path = path.resolve(strict=True)
        stat = path.stat()
        self._size = stat.st_size
        self._mtime = stat.st_mtime_ns
        self._hash: Optional[bytes] = None
        self._fs_id: Optional[int] = None

    def as_sql_dict(self, include_hash: bool=False) -> dict:
        """
        Returns all metadata about the file as a dict.

        This equates to creating a dict that stores every attribute of the
        object. Notably, the `path` key is represented by a string, not a
        `Path` object.

        Args:
          include_hash:
            Whether to include the hash in the dict. If `False`, the hash is
            `None`. Set to `False` if you don't need the hash, as it requires
            reading the entire file from disk.
        """
        return {
            "path": str(self.path),
            "size": self.size,
            "mtime": self.mtime,
            "hash": self.hash if include_hash else None,
            "fs_id": self.fs_id
        }

    @property
    def hash(self) -> bytes:
        """The hash of the file as a `bytes` object."""
        if self._hash is not None:
            return self._hash

        print(f"Hashing {self._path}...")
        sha = hashlib.sha256()
        with self._path.open(mode="rb") as f:
            while True:
                data = f.read(CHUNK_SIZE)
                if data:
                    sha.update(data)
                else:
                    break
            self._hash = sha.digest()
            return self._hash

    @property
    def path(self) -> Path:
        """The path of the file, as a `Path` object."""
        return self._path

    @property
    def size(self) -> int:
        """The size of the file in bytes."""
        return self._size

    @property
    def mtime(self) -> int:
        """The time of last modification of the file in nanoseconds."""
        return self._mtime

    @property
    def fs_id(self) -> int:
        """An int representing a unique ID for the filesystem the file is on."""
        if self._fs_id is not None:
            return self._fs_id

        self._fs_id = utils.get_fsid(self._path)
        return self._fs_id

class DbFileMetadata(FileMetadata):
    """
    Creates an object representing a file from precomputed metadata.

    This class is a subclass of `FileMetadata`, however instead of using a file
    on disk as the basis for the stored metadata, this class is constructed from
    a dict of preexisting metadata, and doesn't require the file to actually
    exist on disk. Because of this, this class never accesses the filesystem.

    Attributes:
        See `FileMetadata`'s attributes.
    """
    def __init__(self, file_dict: dict) -> None:
        """
        Inits a `DbFileMetadata` object based upon values in given `file_dict`.

        This class is the inverse of `FileMetadata.as_sql_dict`. Care should
        be given to ensure that all properties of the file's metadata are
        properly furnished in the given `file_dict`. See the attributes of
        `FileMetadata` for a full list of what keys should exist on `file_dict`.
        """
        if not isinstance(file_dict, dict):
            raise TypeError("file_dict given wasn't of type dict.")
        if not isinstance(file_dict["path"], str):
            raise TypeError("file_dict['path'] wasn't a string.")
        if not isinstance(file_dict["hash"], bytes):
            raise TypeError("file_dict['hash'] wasn't of type bytes.")
        if not isinstance(file_dict["size"], int):
            raise TypeError("file_dict['size'] wasn't of type int.")
        if not isinstance(file_dict["mtime"], int):
            raise TypeError("file_dict['mtime'] wasn't of type int.")
        if not isinstance(file_dict["fs_id"], int):
            raise TypeError("file_dict['fs_id'] wasn't of type int.")

        self._path = Path(file_dict["path"])
        self._hash = file_dict["hash"]
        self._size = file_dict["size"]
        self._mtime = file_dict["mtime"]
        self._fs_id = file_dict["fs_id"]

    # We should never need to calculate a hash from a DbFile
    @property
    def hash(self) -> bytes:
        """See base class."""
        return self._hash

    @property
    def fs_id(self) -> int:
        """See base class."""
        return self._fs_id
