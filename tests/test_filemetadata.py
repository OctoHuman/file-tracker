import unittest
from pathlib import Path
from file_tracker.file_metadata import DbFileMetadata, FileMetadata
from tests import utils


class TestFileMetadata(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        utils.set_utimes_on_test_files()
        self.expected_files = utils.get_expected_files()

    def test_filemetadata_constructor(self):
        expected_files = list(self.expected_files) # Make a copy

        for file in expected_files:
            file_path = Path(file["path"])
            fm = FileMetadata(file_path)

            file["path"] = str(Path(file["path"]).resolve()) # Convert path to resolved string.

            self.assertIsInstance(fm.path, Path)
            self.assertIsInstance(fm.size, int)
            self.assertIsInstance(fm.mtime, int)
            self.assertIsInstance(fm.hash, bytes)
            self.assertIsInstance(fm.fs_id, int)

            self.assertEqual(str(fm.path), str(file_path.resolve()))
            self.assertEqual(fm.size, file["size"])
            self.assertEqual(fm.mtime, file["mtime"])
            self.assertEqual(fm.hash, file["hash"])
            # We can't verify fs_id because it varies per system.

            sql_dict = fm.as_sql_dict(include_hash=True)
            sql_dict["fs_id"] = file["fs_id"]

            self.assertEqual(sql_dict, file)

if __name__ == "__main__":
    unittest.main()
