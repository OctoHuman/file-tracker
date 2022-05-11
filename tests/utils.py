import json
import os
from pathlib import Path

def set_utimes_on_test_files() -> None:
    """Sets up utimes for files in test resources."""
    # This is a pretty bad hack, but I need to make sure we are in the right dir.
    if "file_tracker" not in str(Path().resolve()) or not Path(".gitignore").is_file():
        raise Exception("We must be in the root of the repo dir to complete this test.")

    with Path("./tests/resources/common/expected.json").open(mode="rt") as f:
        expected_files = json.load(f)

    for file in expected_files:
        file_path = file["path"]
        file_mtime_ns = file["mtime"]
        os.utime(file_path, ns=(0, file_mtime_ns))

def get_expected_files() -> list[dict[str, str|int|bytes]]:
    """Parses the JSON at ./tests/resources/common/expected.json and returns it"""
    with Path("./tests/resources/common/expected.json").open(mode="rt") as f:
        file_objs = json.load(f)

    for file in file_objs:
        file["hash"] = bytes.fromhex(file["hash"])

    return file_objs