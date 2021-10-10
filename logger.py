"""
This module allows for easy logging to disk.
"""

from pathlib import Path
import time
import sys
from traceback import format_exception

class Logger:
    """
    Logs info from program runtime to disk.

    This class lets you log text, and classify log entries as `log`, `warn`, or
    `error`, depending on severity. In addition, if this class is used with
    Python's `with` syntax, any Exceptions raised inside of the `with` block
    will be logged (but not caught).

    Attributes:
        mirror_to_stdout:
          A `bool` representing whether or not to write all log entries to
          stdout. This attribute can be changed even after the class is inited.
    """
    def __init__(self,
                 log_file: Path,
                 log_exception: bool=True,
                 mirror_to_stdout: bool=False
                ) -> "Logger":
        """
        Inits a Logger for the given `log_file`.

        Arguments:
            log_file:
              A `Path` object representing where to store the log file.
            log_exceptions:
              A `bool` representing whether to log exceptions that occur within
              the `with` block the `Logger` is instantiated from. Does nothing
              if this `Logger` wasn't created using the `with` syntax.
            mirror_to_stdout:
              If `True`, writes all log entries to sys.stdout in addition to the
              usual log file.

        Raises:
            FileExistsError:
              The `log_file` given already exists.
        """
        if not isinstance(log_file, Path):
            raise TypeError("Log file must be of type path.")
        if log_file.exists():
            raise FileExistsError(f"Log file path already exists. Won't clobber. {log_file}")

        self._mirror_to_stdout = bool(mirror_to_stdout)
        self._log_exception = bool(log_exception)
        self._closed = False

        self._fd = log_file.open(mode="xt", encoding="utf8")
        self._fd.write(f"[{get_time()}] START OF LOG\n")

    def __enter__(self) -> "Logger":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if self._closed:
            return

        if exc_type and self._log_exception:
            tb = format_exception(exc_type, exc_value, traceback)
            tb = "".join(tb)
            self.error(tb)
            self.error("Closing due to uncaught exception.")
        self.close()

    def _write(self, text: str, mirror_to_stdout: bool=False) -> None:
        """Lets us write to log without any severity prefixes."""
        if self._closed:
            raise ValueError("Can't write to log because it is already closed.")

        self._fd.write(f"[{get_time()}] {text}")
        if self._mirror_to_stdout or mirror_to_stdout:
            sys.stdout.write(text)

    @property
    def mirror_to_stdout(self) -> bool:
        """A `bool` representing whether to write log entries to sys.stdout."""
        return self._mirror_to_stdout

    @mirror_to_stdout.setter
    def mirror_to_stdout(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise TypeError("mirror_to_stdout must be a bool.")
        self._mirror_to_stdout = value

    def log(self, text: str, **kwargs) -> None:
        """
        Logs text.

        Arguments:
            mirror_to_stdout:
              See `Logger.mirror_to_stdout`.
        """
        self._write(text + "\n", **kwargs)

    def warn(self, text: str, **kwargs) -> None:
        """
        Logs a warning.

        Arguments:
            mirror_to_stdout:
              See `Logger.mirror_to_stdout`.
        """
        self._write("WARNING: " + text + "\n", **kwargs)

    def error(self, text: str, **kwargs) -> None:
        """
        Logs an error.

        Arguments:
            mirror_to_stdout:
              See `Logger.mirror_to_stdout`.
        """
        self._write("ERROR: " + text + "\n", **kwargs)

    def close(self) -> None:
        """Close the log file."""
        if self._closed:
            return

        self._fd.write(f"[{get_time()}] END OF LOG\n")
        self._fd.close()
        self._closed = True

def get_time() -> str:
    """Returns the current time in a nicely formatted string."""
    return time.strftime("%Y-%m-%d %H:%M:%S")
