from pathlib import Path
from file_metadata import FileMetadata
import csv
import gzip

CSV_HEADER = ["action", "reason", "path", "new_hash"]

# This dict maps log actions to reason lists
LOG_ACTIONS = {
    "new": [
        "new_file" # When a new file is found that isn't in the database
    ],

    "update": [
        "changed" # File is suspected to have been changed
    ],

    "delete": [
        "nonexistent", # File from database couldn't be found on disk
        "invalid_fs_id" # File from database is in a fsid no longer being tracked
    ],
    
    "skip": [
        "unchanged" # File doesn't appear to have been changed
    ],

    "error": [
        "unexpected_fs_id" # File inside of registered filesystem doesn't match expected fsid
    ]
}

# "skip", "unexpected_fsid", path
# "new", "new_file", path, new_hash
# "updated", "changed", path, new_hash
# "skip", "unchanged", path 
# "delete", "bad_fsid", path
# "delete", "nonexistent", path

class FileMetadataHistoryLog:
    def __init__(self, log_path, gzip_compress=True):
        if not isinstance(log_path, Path):
            raise TypeError("log_path must be a Path object.")
        
        self._log_path = log_path
        self._closed = False

        if self._log_path.exists():
            raise FileExistsError(f"Can't create a new log at '{self._log_path}': File already exists.")
        
        if gzip_compress:
            self._fd = gzip.open(self._log_path, mode="xt", encoding="utf8", newline="")
        else:
            self._fd = self._log_path.open(mode="xt", encoding="utf8", newline="")
        
        self._csv_writer = csv.DictWriter(self._fd, fieldnames=CSV_HEADER)
        self._csv_writer.writeheader()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def add(self, action, reason, file):
        if self._closed:
            raise ValueError("Can't write new log entry: log has been closed.")
        
        if not action in LOG_ACTIONS:
            raise ValueError(f"Invalid log action: {action}")
        if not reason in LOG_ACTIONS[action]:
            raise ValueError(f"Invalid log reason '{reason}' for action '{action}'")
        if not isinstance(file, FileMetadata):
            raise TypeError("File argument must be a FileMetadata object.")

        csv_dict = {
            "action": action,
            "reason": reason,
            "path": str(file.path)
        }

        if action == "new" or action == "update":
            csv_dict["new_hash"] = file.hash.hex()

        self._csv_writer.writerow(csv_dict)
    
    def close(self):
        if self._closed:
            return
        
        self._fd.close()
        self._closed = True