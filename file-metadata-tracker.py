"""
Track metadata by logging file hash, size, and mod time (possibly also extended attributes).
Allow scanning of files to update this database when file info changes.
Furthermore, then be able to scan this database to determine likely file duplicates.
If a potential duplicate is found, verifiy that file matches by doing a byte-by-byte comparison, finally replacing the duplicate with a hardlink.
"""