from pathlib import Path
import time
import sys
from traceback import format_exception

class Logger:
    def __init__(self, log_file, log_exception=True, mirror_to_stdout=False):
        if not isinstance(log_file, Path):
            raise TypeError("Log file must be of type path.")
        if log_file.exists():
            raise FileExistsError(f"Log file path already exists. Won't clobber. {log_file}")
        
        self._mirror_to_stdout = bool(mirror_to_stdout)
        self._log_exception = bool(log_exception)
        self._closed = False

        self._fd = log_file.open(mode="xt", encoding="utf8")
        self._fd.write(f"[{get_time()}] START OF LOG\n")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        if self._closed:
            return

        if exc_type and self._log_exception:
            tb = format_exception(exc_type, exc_value, traceback)
            tb = "".join(tb)
            self.error(tb)
            self.error("Closing due to uncaught exception.")
        self.close()

    def _write(self, text, mirror_to_stdout=False):
        if self._closed:
            raise ValueError("Can't write to log because it is already closed.")
        
        self._fd.write(f"[{get_time()}] {text}")
        if self._mirror_to_stdout or mirror_to_stdout:
            sys.stdout.write(text)
    
    @property
    def mirror_to_stdout(self):
        return self._mirror_to_stdout
    
    @mirror_to_stdout.setter
    def mirror_to_stdout(self, value):
        if not isinstance(value, bool):
            raise TypeError("mirror_to_stdout must be a bool.")
        self._mirror_to_stdout = value

    def log(self, text, **kwargs):
        self._write(text + "\n", **kwargs)
    
    def warn(self, text, **kwargs):
        self._write("WARNING: " + text + "\n", **kwargs)

    def error(self, text, **kwargs):
        self._write("ERROR: " + text + "\n", **kwargs)

    def close(self):
        if self._closed:
            return
        
        self._fd.write(f"[{get_time()}] END OF LOG\n")
        self._fd.close()
        self._closed = True

def get_time():
    return time.strftime("%Y-%m-%d %H:%M:%S")