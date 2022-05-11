import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from file_tracker.file_metadata import DbFileMetadata
from file_tracker.file_metadata_db import FileMetadataDb
from tests import utils


class TestFileMetadataDb(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.expected_files = [DbFileMetadata(x) for x in utils.get_expected_files()]

    def setUp(self):
        """Automatically creates a unique `self.db_file` for each test run."""
        # Deprecated, but I need a non-existant file, not one created for me
        self.db_file = Path(tempfile.mktemp()) # Path to temp file
        self.db = None

    def tearDown(self):
        """We automatically close `self.db` and delete `self.db_file` after every test run."""
        if self.db is not None:
            self.db.close()

        # Clean up temporary db_file, even if a test fails
        self.db_file.unlink(missing_ok=True)

    def create_new_database(self) -> FileMetadataDb:
        """Returns a database created at path `self.db_file`"""
        return FileMetadataDb(self.db_file, readonly=False, create_new_db=True)

    def test_constructor(self):
        self.db = FileMetadataDb(self.db_file, readonly=False, create_new_db=True)
        self.db.close()

        # The `with` keyword should be supported
        with FileMetadataDb(self.db_file) as self.db:
            self.db.close()

        with self.assertRaises(FileExistsError):
            # We shouldn't be able to clobber the existing database.
            self.db = FileMetadataDb(self.db_file, readonly=False, create_new_db=True)

        self.db_file.unlink() # Delete database

        with self.assertRaises(FileNotFoundError):
            # We shouldn't be able to open a nonexistant db.
            # db_file points to nonexistent file.
            self.db = FileMetadataDb(self.db_file)

        with self.assertRaises(ValueError):
            # You shouldn't be able to create a database in readonly mode.
            self.db = FileMetadataDb(self.db_file, readonly=True, create_new_db=True)

        with self.assertRaises(TypeError):
            # db_path must be either a string or Path object.
            self.db = FileMetadataDb(123, readonly=True, create_new_db=False)

    def test_get_file(self):
        self.db = FileMetadataDb("tests/resources/test_file_metadata_db/common/basic_db.db")

        with self.assertRaises(TypeError):
            # file_metadata must be a FileMetadata object.
            self.db.get_file("abc")

        for file in self.expected_files:
            result = self.db.get_file(file)
            self.assertIsInstance(result, DbFileMetadata)
            self.assertEqual(str(file.path), str(result.path))
            self.assertEqual(file, result)

    def test_does_exist(self):
        self.db = FileMetadataDb("tests/resources/test_file_metadata_db/common/basic_db.db")

        with self.assertRaises(TypeError):
            self.db.does_exist("abc123")

        with self.assertRaises(TypeError):
            self.db.does_exist(None)

        for file in self.expected_files:
            did_exist = self.db.does_exist(file)
            self.assertIsInstance(did_exist, bool)
            self.assertTrue(did_exist)

        shouldnt_exist = DbFileMetadata({
            "path": "tests/resources/common/test_files/file3.txt",
            "size": 9,
            "hash": bytes.fromhex("b1ab25c55913c95cc6913f1dbce9bef185ebf00a64553a8ef194193e52ea5015"),
            "mtime": 1645312127724000000,
            "fs_id": 123
        })

        did_exist = self.db.does_exist(shouldnt_exist)
        self.assertIsInstance(did_exist, bool)
        self.assertFalse(did_exist)


    def test_add_file(self):
        self.db = self.create_new_database()

        with self.assertRaises(TypeError):
            # file_metadata should be a FileMetadata object.
            self.db.add_file("testing")

        fake_file = DbFileMetadata({
            "path": "tests/resources/common/test_files/filexyz.txt",
            "size": 9991,
            "hash": bytes.fromhex("b1ab25c55913d95bc691331dbce9bffff5ebf00a64553a8ef194193e52ea5015"),
            "mtime": 1141212117524000000,
            "fs_id": 234
        })

        self.db.add_file(fake_file)
        self.db.commit()
        self.db.close()

        self.db = FileMetadataDb(self.db_file, readonly=False)

        result = self.db.get_file(fake_file)
        self.assertEqual(result, fake_file)

        fake_file2 = DbFileMetadata({
            "path": "tests/resources/common/test_files/filexyz.txt",
            "size": 9999,
            "hash": bytes.fromhex("aaaa25c55913d95bc691331dbce9bffff5ebf00a64553a8ef194193e52ea5015"),
            "mtime": 2141212117524000000,
            "fs_id": 567
        })

        with self.assertRaises(sqlite3.IntegrityError):
            # Because `fake_file2` has the same path as `fake_file`, we
            # shouldn't be able to `add_file` on it without the SQL database
            # raising an error to point out the collision.
            self.db.add_file(fake_file2)

        self.db.commit()
        self.db.close()
        self.db = FileMetadataDb(self.db_file, readonly=True)

        with self.assertRaises(RuntimeError):
            # Database is in read only mode, so this should fail
            self.db.add_file(fake_file)

    def test_update_file(self):
        self.db = self.create_new_database()

        orig_file = DbFileMetadata({
            "path": "tests/resources/common/test_files/filexyz.txt",
            "size": 9991,
            "hash": bytes.fromhex("b1ab25c55913d95bc691331dbce9bffff5ebf00a64553a8ef194193e52ea5015"),
            "mtime": 1141212117524000000,
            "fs_id": 234
        })

        updated_file = DbFileMetadata({
            "path": "tests/resources/common/test_files/filexyz.txt",
            "size": 888,
            "hash": bytes.fromhex("aaaaaac55913d95bc691331dbce9bffff5ebf00a64553a8ef194193e52ea5015"),
            "mtime": 1111123417524000000,
            "fs_id": 456
        })

        with self.assertRaises(FileNotFoundError):
            #Can't update a file that doesn't already exist in the database.
            self.db.update_file(updated_file)

        with self.assertRaises(TypeError):
            #file_metadata must be a `FileMetadata` object.
            self.db.update_file("testing")

        self.db.add_file(orig_file)
        self.db.update_file(updated_file)

        self.assertEqual(self.db.get_file(updated_file), updated_file)
        # Because get_file looks up the file based on path, these should be
        # interchangable
        self.assertEqual(self.db.get_file(orig_file), updated_file)

        self.db.commit()
        self.db.close()

        self.db = FileMetadataDb(self.db_file, readonly=True)
        with self.assertRaises(RuntimeError):
            #Can't update a file when database is in read only mode.
            self.db.update_file(updated_file)

    def test_remove_file(self):
        self.db = self.create_new_database()

        fake_file = DbFileMetadata({
            "path": "tests/resources/common/test_files/filexyz.txt",
            "size": 9991,
            "hash": bytes.fromhex("b1ab25c55913d95bc691331dbce9bffff5ebf00a64553a8ef194193e52ea5015"),
            "mtime": 1141212117524000000,
            "fs_id": 234
        })

        self.db.add_file(fake_file)

        self.assertTrue(self.db.does_exist(fake_file))
        self.db.remove_file(fake_file)
        self.assertFalse(self.db.does_exist(fake_file))

        with self.assertRaises(FileNotFoundError):
            # An exception should be raised when trying to remove a file that
            # doesn't exist in the database.
            self.db.remove_file(fake_file)

        with self.assertRaises(TypeError):
            #file_metadata must be a `FileMetadata` object.
            self.db.remove_file("testing")

        self.db.close()
        self.db = FileMetadataDb(self.db_file, readonly=True)
        with self.assertRaises(RuntimeError):
            # We shouldn't be able to remove a file when database is read only.
            self.db.remove_file(fake_file)


    def test_get_all_files(self):
        self.db = FileMetadataDb("tests/resources/test_file_metadata_db/common/basic_db.db")

        result_files = []
        for file in self.db.get_all_files():
            self.assertIsInstance(file, DbFileMetadata)
            result_files.append(file)

        result_files.sort(key=lambda x: str(x.path))

        expected_results = list(self.expected_files) # Make a copy of expected_files
        expected_results.sort(key=lambda x: str(x.path))

        self.assertEqual(len(result_files), len(expected_results))

        for i in range(len(result_files)):
            self.assertEqual(result_files[i], expected_results[i])

    def test_get_files_matching_hash(self):
        self.db = FileMetadataDb("tests/resources/test_file_metadata_db/common/basic_db.db")
        hash_to_match = bytes.fromhex("4bea3e0214a5d4ff2b9cf9badb4d007b079d649a85117a74f1e04d47a875abbe")

        expected_results = [
            "tests/resources/common/test_files/file with spaces 2.txt",
            "tests/resources/common/test_files/folder1/folder with spaces/file with spaces.txt"
        ]

        result_files = []
        for file in self.db.get_files_matching_hash(hash_to_match):
            self.assertIsInstance(file, DbFileMetadata)
            path_str = str(file.path)
            result_files.append(path_str.replace("\\", "/")) # Handle cross platform separators

        self.assertCountEqual(expected_results, result_files)

        hash_with_no_matches = bytes.fromhex("a" * 64)
        for file in self.db.get_files_matching_hash(hash_with_no_matches):
            raise ValueError("There should never be a match to this hash.")

        with self.assertRaises(ValueError):
            # Hash must be passed as `bytes`.
            for file in self.db.get_files_matching_hash("a" * 32):
                pass

        with self.assertRaises(ValueError):
            # Hash must be exactly 32 bytes.
            for file in self.db.get_files_matching_hash(bytes([20] * 64)):
                pass

    def test_get_files_matching_regex(self):
        self.db = FileMetadataDb("tests/resources/test_file_metadata_db/common/basic_db.db")

        expected_results = [
            "tests/resources/common/test_files/file1.txt",
            "tests/resources/common/test_files/file2.txt",
            "tests/resources/common/test_files/file with spaces 2.txt",
            "tests/resources/common/test_files/folder1/folder with spaces/file with spaces.txt"
        ]

        result_files = []
        # Only find files who's name starts with "file"
        for file in self.db.get_files_matching_regex(r"^.+\\file.+$"):
            path_str = str(file.path)
            result_files.append(path_str.replace("\\", "/")) # Handle cross platform separators

        self.assertCountEqual(expected_results, result_files)

        for file in self.db.get_files_matching_regex(r"nonexistant"):
            raise ValueError("This regex should never match anything.")

    def test_commit(self):
        self.db = self.create_new_database()

        # It's hard to tell if it actually commited, so the best we can
        # do is make sure it doesn't raise any expctions.

        self.db.commit()
        self.db.commit()

    def test_close(self):
        self.db = self.create_new_database()

        self.db.close()
        # We should be able to call close as many times as we want with no errors
        self.db.close()
        self.db.close()

    def test_db_path(self):
        self.db = self.create_new_database()

        self.assertEqual(str(self.db_file.resolve()), self.db.db_path)


if __name__ == "__main__":
    unittest.main()
