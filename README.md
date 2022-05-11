# file-tracker
A set of Python scripts to track all of your files' metadata, and log them into a SQLite database.

# Setup
## Create your config
First, you must create a config file using the `update-config` subcommand.

```bash
file-tracker update-config config.json --new --database-path "./path/to/database.db" --log-folder "./path/to/folder/containing/logs" --register-fs "./path/to/fs/to/track" 
```

If you change your mind later, you can always use the `update-config` subcommand to add, remove, or change properties of your config:

```bash
file-tracker update-config config.json --delete-fs "./stop/tracking/this/folder"
```

Or

```bash
file-tracker update-config config.json --log-folder "./new/log/folder"
```

## Update your database 
Finally, run the `update-database` subcommand to scan your filesystem and add the metadata to the database:
```bash
file-tracker update-database config.json
```
