"""
Handles creation and modification of file metadata in a centralized database.
"""

import re
import sqlite3
import sys
from pathlib import Path
from sqlite3.dbapi2 import Connection
from typing import Generator, Optional

from file_tracker.file_metadata import DbFileMetadata, FileMetadata


class FileMetadataDb:
    """
    Connects to or optionally creates a new file metadata database.

    This file metadata database tracks data such as file path, hash,
    modtime, and size. Methods allow for easy lookup and modification of
    the custom SQLite database. Most functions expect a `FileMetadata`
    object to operate on.

    Attributes:
        readonly:
          A bool representing whether database is in readonly mode.
        db_path:
          A `Path` of where the database exists.
        in_transaction:
          A bool representing whether there are uncommited changes to the
          database.
    """

    def __init__(self,
                 db_path: Path,
                 readonly: bool=True,
                 create_new_db: bool=False
                ) -> None:
        """
        Opens an existing database, or creates and initializes a new
        one.

        Args:
            db_path:
              Path to existing database, or if `create_new_db` is true, a
              location to create the database.
            readonly:
              Whether we should open an existing database in readonly mode. Must
              be false if `create_new_db` is true.
            create_new_db:
              Whether to create a new database. If true, the `db_path` given
              must not point to an existing file.

        Returns:
            A FileMetadataDb object.

        Raises:
            ValueError:
              `readonly` was set to true, but you're trying to create a new
              database.
            FileExistsError:
              Raised when attempting to create a new database, but file already
              exists at given `db_path`.
            FileNotFoundError:
              Raised when attempting to open an existing database, but no file
              exists at given `db_path`.
        """

        if not isinstance(db_path, Path) and not isinstance(db_path, str):
            raise TypeError("db_path must be either a string or Path object.")

        # Only resolve strictly when database already exists.
        self._db_path = Path(db_path).resolve(strict=not create_new_db)
        self._readonly = bool(readonly)
        self._is_closed = False

        if create_new_db:
            self._conn = self._bootstrap_new_db(self._db_path)
        else:
            self._conn = self._connect_to_existing_db(self._db_path)

        self._conn.row_factory = sqlite3.Row
        self._conn.create_function("REGEXP", 2, FileMetadataDb._sql_regex, deterministic=True)

    def __enter__(self) -> "FileMetadataDb":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def get_file(self, file_metadata: FileMetadata) -> DbFileMetadata | None:
        """
        Finds a file in the database matching the path of given `file_metadata`.
        """
        if not isinstance(file_metadata, FileMetadata):
            raise TypeError("File given isn't a FileMetadata object.")

        cur = self._conn.cursor()
        cur.execute("SELECT * FROM files WHERE path = :path", file_metadata.as_sql_dict())
        result: Optional[sqlite3.Row] = cur.fetchone()
        cur.close()
        if result is None:
            return None
        else:
            return DbFileMetadata(result)

    def does_exist(self, file_metadata: FileMetadata) -> bool:
        """
        Checks if a file exists in the database matching the path of given
        `file_metadata`.
        """
        if not isinstance(file_metadata, FileMetadata):
            raise TypeError("File given isn't a FileMetadata object.")

        cur = self._conn.cursor()
        cur.execute("SELECT 1 FROM files WHERE path = :path", file_metadata.as_sql_dict())
        result: Optional[sqlite3.Row] = cur.fetchone()
        cur.close()
        return result is not None

    def add_file(self, file_metadata: FileMetadata) -> None:
        """Adds the file `file_metadata` to the database."""
        # TODO: Should we catch the case if we insert an already existing
        # file into the database again? If we try, _conn.execute raises
        # sqlite3.IntegrityError: UNIQUE constraint failed: files.path
        if self._readonly:
            raise RuntimeError("Can't add a new file while in read-only mode.")
        if not isinstance(file_metadata, FileMetadata):
            raise TypeError("File given isn't a FileMetadata object.")

        sql_dict = file_metadata.as_sql_dict(include_hash=True)
        self._conn.execute("INSERT INTO files (path, hash, size, mtime, fs_id) VALUES (:path, :hash, :size, :mtime, :fs_id)", sql_dict)

    def update_file(self, file_metadata: FileMetadata) -> None:
        """
        Updates an existing file's metadata based on given `file_metadata`.
        """
        if self._readonly:
            raise RuntimeError("Can't update a file while in read-only mode.")
        if not isinstance(file_metadata, FileMetadata):
            raise TypeError("File given isn't a FileMetadata object.")

        cur = self._conn.cursor()
        sql_dict = file_metadata.as_sql_dict(include_hash=True)
        cur.execute("UPDATE files SET hash = :hash, size = :size, mtime = :mtime, fs_id = :fs_id WHERE path = :path", sql_dict)
        cur.close()
        if cur.rowcount < 1:
            raise FileNotFoundError(f"Updating file failed because it doesn't exist in the database: {file_metadata.path}")

    def remove_file(self, file_metadata: FileMetadata) -> None:
        """
        Removes a file from the database, based on path in given
        `file_metadata`.
        """
        if self._readonly:
            raise RuntimeError("Can't remove a file while in read-only mode.")
        if not isinstance(file_metadata, FileMetadata):
            raise TypeError("File given isn't a FileMetadata object.")

        cur = self._conn.cursor()
        sql_dict = file_metadata.as_sql_dict()
        cur.execute("DELETE FROM files WHERE path = :path", sql_dict)
        cur.close()
        if cur.rowcount < 1:
            raise FileNotFoundError(f"Deleting file failed because it doesn't exist in the database: {file_metadata.path}")

    def get_all_files(self) -> Generator[DbFileMetadata, None, None]:
        """Returns a generator that yields every file in the database."""
        yield from self._execute_and_yield_files("SELECT * FROM files")

    def get_files_matching_hash(self,
                                file_hash: bytes
                               ) -> Generator[DbFileMetadata, None, None]:
        """Finds all files in database that match given hash."""
        if not isinstance(file_hash, bytes):
            raise ValueError("Hash must be a bytes object.")

        #TODO: We enforce the hash size here, but not on add_file.
        if len(file_hash) != 32:
            raise ValueError("Hash must be exactly 32 bytes (SHA256).")

        yield from self._execute_and_yield_files(
            "SELECT * FROM files WHERE hash = ?",
            args=(file_hash,)
        )

    def get_files_matching_regex(self,
                                 regex: re.Pattern
                                ) -> Generator[DbFileMetadata, None, None]:
        """Finds all files in database where their path matches given regex."""
        #if not isinstance(regex, re.Pattern):
        #    raise TypeError("Argument 'regex' must be of type re.Pattern.")

        yield from self._execute_and_yield_files(
            "SELECT * FROM files WHERE path REGEXP ?",
            args=(regex,)
        )

    def commit(self) -> None:
        """Commits all changes to database."""
        self._conn.commit()

    def close(self) -> None:
        """Closes the database."""
        if self._is_closed:
            return

        if self._conn.in_transaction:
            print("WARNING: Closing database with unsaved transactions.", file=sys.stderr)
        self._conn.close()
        self._is_closed = True

    def _execute_and_yield_files(self,
                                 query: str,
                                 args: Optional[tuple]=None
                                ) -> Generator[DbFileMetadata, None, None]:
        """
        Helper function used to execute given `query`, and yield all files
        returned as `DbFileMetadata` objects.
        NOTE: This function only allows queries that begin with `SELECT *`.

        Args:
            query: SQL query to execute on this database.
            args: A tuple of items to be substituted into any `?`s in the query.

        Yields:
            `DbFileMetadata` objects representing files that matched the given
            query.
        """
        if not isinstance(query, str):
            raise TypeError("Argument 'query' must be a string.")
        if not isinstance(args, tuple) and args is not None:
            raise TypeError("Argument 'args' must be a tuple or None.")

        # This isn't foolproof, but it's a good start to preventing
        # queries that could modify the database from being used with this
        # function. In addition, we must ensure that the query selects all
        # columns of the matching files, because if we are missing any
        # properties than when we convert it to a `DbFileMetadata` object
        # we will cause an exception.
        if not query.lower().startswith("select * "):
            raise ValueError("Query must begin with `SELECT * `.")

        cur = self._conn.cursor()
        if args is not None:
            cur.execute(query, args)
        else:
            cur.execute(query)

        file: sqlite3.Row
        for file in cur:
            yield DbFileMetadata(file)

        cur.close()

    def _bootstrap_new_db(self, db_path: Path) -> Connection:
        if self._readonly:
            raise ValueError("Can't create a new database while in read-only mode.")
        if db_path.exists():
            raise FileExistsError("Database path given already exists. Won't clobber.")

        # Setup the required tables for the script to work.

        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE files (
                path text primary key not null,
                hash blob not null,
                size int not null,
                mtime int not null,
                fs_id int not null
            );
        """)
        conn.commit()
        return conn

    def _connect_to_existing_db(self, db_path: Path) -> Connection:
        if not db_path.is_file():
            raise FileNotFoundError(f"Database path given doesn't exist: {db_path}")
        uri = db_path.as_uri()
        if self._readonly:
            uri += "?mode=ro"
        return sqlite3.connect(uri, uri=True)

    @staticmethod
    def _sql_regex(regex: str, item: str) -> bool:
        """Handles the custom 'REGEXP' function registered on self._conn."""
        if not isinstance(regex, str):
            raise TypeError("Regex expression must be a string.")
        if not isinstance(item, str):
            raise TypeError("Regex must be applied to a string.")
        try:
            return re.match(regex, item) is not None
        except re.error as err:
            raise ValueError("Regex given to SQL query is invalid.")

    @property
    def db_path(self) -> str:
        """The path the database is stored at."""
        return str(self._db_path)

    @property
    def readonly(self) -> bool:
        """Whether the database is in readonly mode."""
        return self._readonly

    @property
    def in_transaction(self) -> bool:
        """Whether there is any uncommited transactions on the database."""
        return self._conn.in_transaction
