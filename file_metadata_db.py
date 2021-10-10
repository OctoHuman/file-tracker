import sqlite3
import sys
from pathlib import Path
from file_metadata import FileMetadata, DbFileMetadata

class FileMetadataDb:
    def __init__(self, db_path, readonly=True, create_new_db=False):
        # Only resolve strictly when database already exists.
        self._db_path = Path(db_path).resolve(strict=not create_new_db)
        self._readonly = bool(readonly)
        self._conn = None
        self._is_closed = False

        if create_new_db:
            self._bootstrap_new_db(db_path)
        else:
            self._connect_to_existing_db(db_path)

        self._conn.row_factory = sqlite3.Row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def get_file(self, file_metadata):
        if not isinstance(file_metadata, FileMetadata):
            raise TypeError("File given isn't a FileMetadata object.")

        cur = self._conn.cursor()
        cur.execute("SELECT * FROM files WHERE path = :path", file_metadata.as_sql_dict())
        result = cur.fetchone()
        cur.close()
        if result is None:
            return None
        else:
            return DbFileMetadata(result)

    def does_exist(self, file_metadata):
        if not isinstance(file_metadata, FileMetadata):
            raise TypeError("File given isn't a FileMetadata object.")

        cur = self._conn.cursor()
        cur.execute("SELECT 1 FROM files WHERE path = :path", file_metadata.as_sql_dict())
        result = cur.fetchone()
        cur.close()
        return result is not None

    def add_file(self, file_metadata):
        if self._readonly:
            raise AssertionError("Can't add a new file while in read-only mode.")
        if not isinstance(file_metadata, FileMetadata):
            raise TypeError("File given isn't a FileMetadata object.")

        sql_dict = file_metadata.as_sql_dict(include_hash=True)
        self._conn.execute("INSERT INTO files (path, hash, size, mtime, fs_id) VALUES (:path, :hash, :size, :mtime, :fs_id)", sql_dict)

    def update_file(self, file_metadata):
        if self._readonly:
            raise AssertionError("Can't update a file while in read-only mode.")
        if not isinstance(file_metadata, FileMetadata):
            raise TypeError("File given isn't a FileMetadata object.")

        cur = self._conn.cursor()
        sql_dict = file_metadata.as_sql_dict(include_hash=True)
        cur.execute("UPDATE files SET hash = :hash, size = :size, mtime = :mtime, fs_id = :fs_id WHERE path = :path", sql_dict)
        cur.close()
        if cur.rowcount < 1:
            raise FileNotFoundError(f"Updating file failed because it doesn't exist in the database: {file_metadata.path}")

    def remove_file(self, file_metadata):
        if self._readonly:
            raise AssertionError("Can't remove a file while in read-only mode.")
        if not isinstance(file_metadata, FileMetadata):
            raise TypeError("File given isn't a FileMetadata object.")

        cur = self._conn.cursor()
        sql_dict = file_metadata.as_sql_dict()
        cur.execute("DELETE FROM files WHERE path = :path", sql_dict)
        cur.close()
        if cur.rowcount < 1:
            raise FileNotFoundError(f"Deleting file failed because it doesn't exist in the database: {file_metadata.path}")

    def get_all_files(self):
        cur = self._conn.cursor()
        cur.execute("SELECT * FROM files")
        while True:
            file = cur.fetchone()
            if file is None:
                break
            yield DbFileMetadata(file)
        cur.close()

    def get_files_matching_hash(self, file_hash):
        if not isinstance(file_hash, bytes):
            raise ValueError("Hash must be a bytes object.")

        cur = self._conn.cursor()
        cur.execute("SELECT * FROM files WHERE hash = ?", (file_hash,))
        while True:
            file = cur.fetchone()
            if file is None:
                break
            yield DbFileMetadata(file)
        cur.close()

    def commit(self):
        self._conn.commit()

    def close(self):
        if self._is_closed:
            return
        
        if self._conn.in_transaction:
            print("WARNING: Closing database with unsaved transactions.", file=sys.stderr)
        self._conn.close()
        self._is_closed = True

    def _bootstrap_new_db(self, db_path):
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
        self._conn = conn

    def _connect_to_existing_db(self, db_path):
        if not db_path.is_file():
            raise FileNotFoundError(f"Database path given doesn't exist: {db_path}")
        uri = db_path.as_uri()
        if self._readonly:
            uri += "?mode=ro"
        self._conn = sqlite3.connect(uri, uri=True)

    @property
    def db_path(self):
        return str(self._db_path)
    
    @property
    def readonly(self):
        return self._readonly

    @property
    def in_transaction(self):
        return self._conn.in_transaction