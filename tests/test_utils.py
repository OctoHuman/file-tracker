import unittest
from pathlib import Path

from file_tracker import utils

class TestUtils(unittest.TestCase):
    def test_assert_sql_safe_chars(self):
        # These are valid sql names, so an error shouldn't be raised.
        utils.assert_sql_safe_chars("abc")
        utils.assert_sql_safe_chars("123abc123--")

        # These should raise an error
        with self.assertRaises(ValueError):
            utils.assert_sql_safe_chars("abc*xyz")

        with self.assertRaises(ValueError):
            utils.assert_sql_safe_chars("hello world")

        with self.assertRaises(TypeError):
            utils.assert_sql_safe_chars(123)

    def test_is_sqlite_db(self):
        with self.assertRaises(FileNotFoundError):
            utils.is_sqlite_db(Path("./nonexistant")) # Doesn't exist

        with self.assertRaises(FileNotFoundError):
            utils.is_sqlite_db(Path(".")) # Shouldn't accept a directory


        valid_db_path = Path("./tests/resources/test_utils/test_is_sqlite_db/minimum_sqlite3_db.db")
        self.assertTrue(utils.is_sqlite_db(valid_db_path))

        invalid_db_path = Path("./tests/resources/test_utils/test_is_sqlite_db/not_a_sqlite3_db")
        self.assertFalse(utils.is_sqlite_db(invalid_db_path))

    def test_get_fsid(self):
        fsid = utils.get_fsid(Path("."))
        self.assertIsInstance(fsid, int)

    def test_walk_files(self):
        walk_dir = Path("./tests/resources/test_utils/test_walk_files/")
        expected_results = set([str(x) for x in walk_dir.rglob("*") if x.is_file()])

        result_files = set()
        for file in utils.walk_files(walk_dir, lambda x: x): # No good way to test error handler.
            result_files.add(str(file))

        self.assertSetEqual(expected_results, result_files)

if __name__ == "__main__":
    unittest.main()
