from io import UnsupportedOperation
from os import path, getcwd
from pathlib import Path
import sqlite3, builtins

_unmodified_open = builtins.open

class SqlDir:
    def __init__(self, name, mode, db_connection, encoding=None):
        self.name = str(Path(name))
        self.mode = mode
        self.db_connection = db_connection
        self._closed = False

        self._is_binary = 'b' in self.mode
        self._encoding = encoding if not self._is_binary else None

        if not self._is_binary and self._encoding is None: self._encoding = 'utf-8'

        self._buffer = b""

        if "r" in self.mode or "a" in self.mode or "+" in self.mode:
            row = self.db_connection.execute(
                "SELECT content FROM files WHERE filename = ?", (str(self.name),)
            ).fetchone()
            if row is not None:
                self._buffer = row[0]
            else:
                self._buffer = b""

        self._pos = len(self._buffer) if "a" in mode else 0

    def _check_closed(self):
        if self._closed:
            raise ValueError("sqldir:: i/o operation on closed file")

    def __iter__(self):
        self._check_closed()
        self._pos = 0
        return self

    def __next__(self):
        self._check_closed()
        if self._pos >= len(self._buffer):
            raise StopIteration

        next_newline = self._buffer.find(b"\n", self._pos)

        if next_newline == -1:
            line = self._buffer[self._pos:]
            self._pos = len(self._buffer)
        else:
            line = self._buffer[self._pos : next_newline + 1]
            self._pos = next_newline + 1

        if self._is_binary:
            return line
        else:
            if self._encoding is None:
                raise ValueError("sqldir:: encoding is not set for decoding")
            return line.decode(self._encoding)

    def write(self, data):
        self._check_closed()
        if "w" not in self.mode and "a" not in self.mode and "+" not in self.mode:
            raise UnsupportedOperation("sqldir:: file not open for writing")

        if not self._is_binary and isinstance(data, str):
            if self._encoding is None:
                raise ValueError("sqldir:: encoding is not set for encoding")
            data = data.encode(self._encoding)
        elif self._is_binary and isinstance(data, str):
            raise TypeError("sqldir:: write() argument must be bytes, not str")

        self._buffer = (
            self._buffer[: self._pos]
            + data
            + self._buffer[self._pos + len(data) :]
        )
        self._pos += len(data)
        return len(data)

    def read(self, size=-1):
        self._check_closed()
        if "r" not in self.mode and "+" not in self.mode:
            raise UnsupportedOperation("sqldir:: file not open for reading")

        if size < 0:
            data = self._buffer[self._pos :]
            self._pos = len(self._buffer)
        else:
            data = self._buffer[self._pos : self._pos + size]
            self._pos += len(data)

        if self._is_binary:
            return data
        else:
            if self._encoding is None:
                raise ValueError("sqldir:: encoding is not set for decoding")
            return data.decode(self._encoding)

    def readline(self, size=-1):
        self._check_closed()
        if "r" not in self.mode and "+" not in self.mode:
            raise UnsupportedOperation("sqldir:: file not open for reading")

        if self._pos >= len(self._buffer):
            return b"" if self._is_binary else ""

        newline_index = self._buffer.find(b"\n", self._pos)

        if newline_index == -1:
            line = self._buffer[self._pos:]
            self._pos = len(self._buffer)
        else:
            line = self._buffer[self._pos : newline_index + 1]
            self._pos = newline_index + 1

        if size >= 0 and len(line) > size:
            line = line[:size]
            self._pos = self._pos - (len(line) - size)

        if self._is_binary:
            return line
        else:
            if self._encoding is None:
                raise ValueError("sqldir:: encoding is not set for decoding")
            return line.decode(self._encoding)

    def readlines(self, hint=-1):
        self._check_closed()
        lines = []
        total_bytes_read = 0

        while True:
            line = self.readline()
            if not line: break

            lines.append(line)
            if self._is_binary:
                total_bytes_read += len(line)
            else:
                if self._encoding is None:
                    raise ValueError("sqldir:: encoding is not set for decoding")
                if not isinstance(line, str):
                    raise ValueError("sqldir:: expected line to be 'str' in text mode")
                total_bytes_read += len(line.encode(self._encoding))
            if hint > 0 and total_bytes_read >= hint:
                break

        return lines

    def seek(self, offset, whence=0):
        self._check_closed()
        match whence:
            case 0: self._pos = offset
            case 1: self._pos += offset
            case 2: self._pos = len(self._buffer) + offset
            case _: pass
        return self._pos

    def tell(self):
        self._check_closed()
        return self._pos

    def close(self):
        if not self._closed:
            self._closed = True
            if any(m in self.mode for m in ["w", "a", "+"]):
                self.db_connection.execute(
                    """
                    INSERT OR REPLACE INTO files (filename, content)
                    VALUES (?, ?)
                    """,
                    (self.name, self._buffer),
                )
                self.db_connection.commit()

    def flush(self):
        self._check_closed()

    def __enter__(self):
        self._check_closed()
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

# = = =

def _is_in_current_directory(filename):
    full_path = path.abspath(filename)
    cwd = path.abspath(getcwd())
    return path.commonpath([cwd, full_path]) == cwd

def open(
    name,
    mode="r",
    buffering=-1,
    encoding=None,
    errors=None,
    newline=None,
    closefd=True,
    opener=None,
    use_sqldir=True,
):
    db_modes = {"w", "r", "a", "r+", "w+", "a+"}
    if (
        not _is_in_current_directory(name)
        or not any(m in mode for m in db_modes)
        or not use_sqldir
    ):
        return _unmodified_open(
            name,
            mode,
            buffering,
            encoding,
            errors,
            newline,
            closefd,
            opener
        )

    return SqlDir(name, mode, _sqldir_connection, encoding=encoding)

def install_patch(path="sqldir.db"):
    global _sqldir_connection
    _sqldir_connection = sqlite3.connect(path)
    _sqldir_connection.execute(
        """
        CREATE TABLE IF NOT EXISTS files (
            filename TEXT PRIMARY KEY,
            content BLOB
        );
        """
    )
    builtins.open = open

def remove_patch():
    builtins.open = _unmodified_open

def cursor():
    if not isinstance(_sqldir_connection, sqlite3.Connection): 
        raise Exception("sqldir:: patch not installed")
    cur = _sqldir_connection.cursor()
    return cur
