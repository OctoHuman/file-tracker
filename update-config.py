"""
This script manages updating an validating file metadata tracking configs.

This script outputs config files as standard JSON. It stores data such as which
filesystems to track, and the path to the file metadata database.
"""

from argparse import ArgumentParser
from pathlib import Path
import time
from math import floor
import json
from file_metadata_db import FileMetadataDb
import os
import sys
import utils

# Should this database track the inode of each file, therefore allowing detection of hardlinks pointing to the same file?

arg_parser = ArgumentParser(
                                description="Updates config for file metadata tracker."
                           )
arg_parser.add_argument("config_file", type=Path, help="Path to config file")
arg_parser.add_argument("--new", action="store_true", help="Create new config file. When using this parameter, you are required to specify at least on filesystem to track.")

arg_parser.add_argument("--database-path", type=Path, help="File metadata database.")
arg_parser.add_argument("--log-folder", type=Path, help="A folder to hold database logs.")

arg_parser.add_argument("--register-fs", action="append", type=Path, help="Add a new filesystem to track.")
arg_parser.add_argument("--delete-fs", action="append", type=Path, help="Delete filesystem from tracking.")

args = arg_parser.parse_args()

current_config = None

def main():
    config_file = args.config_file
    create_new_config = args.new
    new_db_path = args.database_path
    new_log_folder = args.log_folder
    add_fs = args.register_fs
    remove_fs = args.delete_fs

    if create_new_config and config_file.exists():    
        raise FileExistsError("Tried to create new config file, but path given already exists. Won't clobber.")
    if not create_new_config and not config_file.is_file():
        raise FileNotFoundError("No file exists at given path.")

    if create_new_config:
        if not new_db_path or not new_log_folder:
            raise ValueError("When creating a new config you must specify a database path and a log folder.")
        current_config = create_config_template()
    else:
        current_config = read_config(config_file)

    if add_fs:
        add_filesystems(current_config, add_fs)
    
    if remove_fs:
        remove_filesystems(current_config, remove_fs)
    
    if new_db_path:
        update_database_path(current_config, new_db_path)
    
    if new_log_folder:
        update_log_folder(current_config, new_log_folder)
    
    current_config["config_last_changed"] = current_time()
    write_config(current_config, config_file)

def add_filesystems(config, add_fs):
    """
    Adds a new filesystem/directory to track to the config.
    
    This is done by storing the path to the filesystem/directory, but also the
    unique filesystem ID, to ensure that if the given filesystem has a mount
    point on it that contains a different filesystem, it is possible to detect
    the entry into a new filesystem.
    """
    print("Registering new filesystems...")
    for fs in add_fs:
        if not fs.is_dir():
            raise ValueError(f"Filesystem '{fs}' isn't a directory.")

        fs_str = str(fs.resolve(strict=True))
        if not fs_str in config["filesystems_to_scan"]:
            config["filesystems_to_scan"][fs_str] = utils.get_fsid(fs)
        else:
            print(f"Warning: Filesystem '{fs}' already exists in config. Ignoring...")

def remove_filesystems(config, remove_fs):
    """Removes a filesystem/directory from the config."""
    print("Removing filesystems...")
    for fs in remove_fs:
        # Don't use 'strict=True' because the filesystem may no longer exist.
        fs_str = str(fs.resolve(strict=False))
        if fs_str in config["filesystems_to_scan"]:
            del config["filesystems_to_scan"][fs_str]
        else:
            print(f"Warning: Filesystem '{fs}' wasn't found in the config. Ignoring...")

def update_database_path(config, database_path):
    """
    Change the path to the file metadata database
    
    Raises:
        ValueError:
          The `database_path` given points to something other than a file, or
          that the file it does point to isn't an SQLite database.
    """
    print("Updating database path...")

    if database_path.exists() and not database_path.is_file():
        raise ValueError("New database doesn't point to a file.")

    if not database_path.exists():
        create_new_database(database_path)
    
    if not utils.is_sqlite_db(database_path):
        raise ValueError("New database path points to a non-database file.")
    
    config["database"] = str(database_path.resolve(strict=True))

def update_log_folder(config, log_folder):
    """Update the folder to store log files in."""
    print("Updating log folder...")

    if not log_folder.is_dir():
        raise NotADirectoryError("Log folder must be a directory.")
    if len(os.listdir(log_folder)) != 0:
        print("WARNING: Log folder already contains files. Are you sure this is correct?")

    config["log_folder"] = str(log_folder.resolve(strict=True))

def read_config(config_file):
    """
    Read the config file at the given path. Returns the parsed JSON file.
    
    Raises:
        ValueError:
          The parsed JSON file doesn't have the correct properties to be a valid
          config file.
    """
    with config_file.open(mode="rt") as f:
        config = json.load(f)
    
    try:
        config["config_last_changed"]
    except KeyError:
        raise ValueError("This doesn't appear to be a valid config file.")
    else:
        return config

def write_config(config, config_file):
    """Dumps the given config file to a string, and writes it to disk."""
    print("Writing new config...")
    with config_file.open(mode="wt") as f:
        json.dump(config, f, indent=4)
    print("Done!")

def create_config_template():
    """Returns a config template."""
    return {
        "config_last_changed": current_time(),
        "database": None,
        "log_folder": None,
        "filesystems_to_scan": {}
    }

def current_time():
    """Returns the current time in milliseconds."""
    return floor(time.time() * 1000)

def create_new_database(database_path):
    """Create a new, empty `FileMetadataDb`."""
    print("Creating new database...")
    # Creates new database, and closes it after it initializes itself.
    with FileMetadataDb(database_path, readonly=False, create_new_db=True) as db:
        db.close()
    print("Done creating database.")

main()