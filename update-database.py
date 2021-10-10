import json
import time
from argparse import ArgumentParser
from pathlib import Path

import logger
import utils
from file_metadata import FileMetadata
from file_metadata_db import FileMetadataDb
from file_metadata_history_log import FileMetadataHistoryLog

arg_parser = ArgumentParser(
                                description="Scans filesystem and updates file metadata database."
                           )
arg_parser.add_argument("config_file", type=Path, help="Path to config file.")

args = arg_parser.parse_args()

files_added = 0
files_updated = 0
files_skipped = 0
files_deleted = 0
files_error = 0

"""
Using Path.rglob doesn't throw errors when it encounters
a directory that I don't have permissions to. Instead, it silently
ignores that directory. I may want to use os.walk to ensure I can
throw errors when I encounter a dir that I can't enter.

I can also add code to Path.rglob that checks every directory I encounter
and ensure that I have read access to it
"""

def main():
    config_file = args.config_file
    config = read_config(config_file)
    db_path = config["db_path"]
    log_folder = config["log_folder"]
    filesystems = config["filesystems"]

    log_paths = create_log_file_paths(log_folder)
    log_file = log_paths["log"]
    history_csv_file = log_paths["csv"]

    with logger.Logger(log_file, mirror_to_stdout=False) as log:
        with FileMetadataHistoryLog(history_csv_file) as history:
            with FileMetadataDb(db_path, readonly=False) as db:
                prune_deleted_files(db, filesystems, log, history)
                register_new_files(db, filesystems, log, history)
                
                # Want the following lines to be printed to the console as well as logged.
                log.mirror_to_stdout = True
                log.log("Finished updating database. Committing changes...")
                db.commit()
            
            log.log(f"Files added: {files_added}")
            log.log(f"Files updated: {files_updated}")
            log.log(f"Files deleted: {files_deleted}")
            log.log(f"Files skipped: {files_skipped}")
            log.log(f"File errors: {files_error}")


def register_new_files(db, filesystems, log, history):
    global files_added, files_skipped, files_updated, files_error
    for fs in filesystems:
        log.log(f"Iterating over filesystem '{fs}'...", mirror_to_stdout=True)
        for file in utils.walk_files(fs, lambda err: log_permission_error(log, err)):
            # `file` could be a block device, network socket, etc.
            if not file.is_file():
                continue
            file_metadata = FileMetadata(file)
            db_file = db.get_file(file_metadata)

            if file_metadata.fs_id != filesystems[fs]:
                log.error(f"Unexpected fsid for '{file}', fsid: '{file_metadata.fs_id}'.")
                log_change(history, "error", "unexpected_fs_id", file_metadata)
                files_error += 1

            elif db_file is None:
                try:
                    db.add_file(file_metadata)
                    log_change(history, "new", "new_file", file_metadata)
                    files_added += 1
                except PermissionError as err:
                    log_permission_error(log, err)

            elif has_file_changed(db_file, file_metadata):
                try:
                    db.update_file(file_metadata)
                    log_change(history, "update", "changed", file_metadata)
                    files_updated += 1
                except PermissionError as err:
                    log_permission_error(log, err)
            
            else:
                log_change(history, "skip", "unchanged", file_metadata)
                files_skipped += 1


def prune_deleted_files(db, filesystems, log, history):
    global files_deleted
    log.log("Pruning database of deleted files...", mirror_to_stdout=True)
    allowed_fsids = filesystems.values()

    for file in db.get_all_files():
        if not file.fs_id in allowed_fsids:
            db.remove_file(file)
            log.warn(f"Found file with invalid fs_id '{file.fs_id}': '{file.path}'. Deleting.")
            log_change(history, "delete", "invalid_fs_id", file)
            files_deleted += 1

        elif not file.path.is_file():
            db.remove_file(file)
            log_change(history, "delete", "nonexistent", file)
            files_deleted += 1
        
    log.log(f"Pruning complete with {files_deleted} files deleted.", mirror_to_stdout=True)

def log_change(history, action, reason, file):
    # A simple helper that adds change to history log,
    # as well as prints the change to the console.

    history.add(action, reason, file)
    print(f"{action.upper()}: {reason}, {file.path}")

def log_permission_error(log, err):
    global files_error
    log.error(f"Permission Error: {err}")
    files_error += 1

def validate_filesystem_mapping(filesystems):
    for filesystem_str in filesystems:
        filesystem = Path(filesystem_str)
        if not filesystem.is_dir():
            raise NotADirectoryError(f"A filesystem specified in the config file is not a directory: {filesystem}")
        
        fsid = utils.get_fsid(filesystem)
        expected_fsid = filesystems[filesystem_str]
        if expected_fsid != fsid:
            raise ValueError(f"fsid mismatch found in config file for filesystem {filesystem}. Expected: {expected_fsid}, actual: {fsid}.")

def has_file_changed(file_metadata_1, file_metadata_2):
    if file_metadata_1.mtime != file_metadata_2.mtime:
        return True
    if file_metadata_1.size != file_metadata_2.size:
        return True
    if file_metadata_1.fs_id != file_metadata_2.fs_id:
        return True
    return False

def read_config(config_file):
    if not config_file.is_file():
        raise FileNotFoundError("Config file specified doesn't exist.")

    with config_file.open(mode="rt") as f:
        config = json.load(f)

    db_path = config["database"]
    if not db_path or not isinstance(db_path, str):
        raise TypeError("No database path is specified in the config file.")
    
    db_path = Path(db_path)
    if not utils.is_sqlite_db(db_path):
        raise ValueError("Database specified in config file isn't a valid SQLite database.")

    log_folder = config["log_folder"]
    if not log_folder or not isinstance(log_folder, str):
        raise ValueError("No log folder specified in config file.")
    
    log_folder = Path(log_folder)
    if not log_folder.is_dir():
        raise NotADirectoryError("Log folder specified in config doesn't exist.")

    filesystems = config["filesystems_to_scan"]
    if not filesystems or not isinstance(filesystems, dict):
        raise ValueError("No filesystems are specified in the config file.")

    validate_filesystem_mapping(filesystems)

    return {
        "db_path": db_path,
        "filesystems": filesystems,
        "log_folder": log_folder
    }

def create_log_file_paths(log_folder):
    if not log_folder.is_dir():
        raise NotADirectoryError("Log folder isn't a directory.")
    log_file_base = log_folder / time.strftime("%Y-%m-%d %H-%M-%S")

    log_file = log_file_base.with_suffix(".log")
    csv_file = log_file_base.with_suffix(".csv.gz")

    if log_file.exists() or csv_file.exists():
        raise FileExistsError("Log file already exists, won't clobber. (Did you run this script twice in one second?)")
    
    return {
        "log": log_file,
        "csv": csv_file
    }

main()
