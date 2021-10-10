from pathlib import Path
import hashlib
import sys
import os
import utils

CHUNK_SIZE = 64000000 #64MB

class FileMetadata:
    def __init__(self, path):
        if not isinstance(path, Path):
            raise TypeError("Path argument isn't a pathlib.Path object.")
        if not path.is_file():
            raise FileNotFoundError(f"Given path '{path}' is not a file.")
        if path.is_symlink():
            raise TypeError(f"Given path points to a symlink, which is unsupported: {path}")
        
        self._path = path.resolve(strict=True)
        stat = path.stat()
        self._size = stat.st_size
        self._mtime = stat.st_mtime
        self._hash = None
        self._fs_id = None

    def as_sql_dict(self, include_hash=False):
        return {
            "path": str(self.path),
            "size": self.size,
            "mtime": self.mtime,
            "hash": self.hash if include_hash else None,
            "fs_id": self.fs_id
        }

    @property
    def hash(self):
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
    def path(self):
        return self._path
    
    @property
    def size(self):
        return self._size
    
    @property
    def mtime(self):
        return self._mtime
    
    @property
    def fs_id(self):
        if self._fs_id is not None:
            return self._fs_id
        
        self._fs_id = utils.get_fsid(self._path)
        return self._fs_id

class DbFileMetadata(FileMetadata):
    def __init__(self, file_dict):
        self._path = Path(file_dict["path"])
        self._hash = file_dict["hash"]
        self._size = file_dict["size"]
        self._mtime = file_dict["mtime"]
        self._fs_id = file_dict["fs_id"]

    # We should never need to calculate a hash from a DbFile
    @property
    def hash(self):
        return self._hash

    @property
    def fs_id(self):
        return self._fs_id