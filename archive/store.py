"""A local folder that behaves like the parts of S3 we rely on.

put_if_absent mirrors PutObject with `If-None-Match: *`.
swap mirrors PutObject with `If-Match: <etag>`.
get_with_etag mirrors GetObject returning the object's ETag.
get_range mirrors GetObject with a `Range:` header.
list mirrors ListObjectsV2: a flat, recursive prefix match.
modified mirrors LastModified.
The same interface runs against real S3 or MinIO in s3store.py.
"""
import fcntl
import hashlib
import os
import shutil
import tempfile
from pathlib import Path


def etag(data):
    return hashlib.sha256(data).hexdigest()


class LocalStore:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key):
        return self.root / key

    def _temp(self, data):
        fd, name = tempfile.mkstemp(dir=self.root, prefix=".tmp-")
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        return name

    def _link_new(self, temp, key):
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.link(temp, path)  # link is atomic and fails if path exists
            return True
        except FileExistsError:
            return False
        finally:
            os.unlink(temp)

    def put_if_absent(self, key, data):
        """Create key. Returns False (S3's 412) if it already exists."""
        return self._link_new(self._temp(data), key)

    def put_file_if_absent(self, key, local_path):
        """Same, streaming from a local file so big segments never sit in memory."""
        fd, temp = tempfile.mkstemp(dir=self.root, prefix=".tmp-")
        with os.fdopen(fd, "wb") as out, open(local_path, "rb") as src:
            shutil.copyfileobj(src, out, 1024 * 1024)
        return self._link_new(temp, key)

    def get(self, key):
        try:
            return self._path(key).read_bytes()
        except FileNotFoundError:
            return None

    def get_with_etag(self, key):
        data = self.get(key)
        return (None, None) if data is None else (data, etag(data))

    def get_range(self, key, start, length=None):
        """Ranged GET. A negative start means the last -start bytes (`Range: bytes=-N`)."""
        try:
            with open(self._path(key), "rb") as f:
                if start < 0:
                    size = f.seek(0, os.SEEK_END)
                    f.seek(max(0, size + start))
                    return f.read()
                f.seek(start)
                return f.read() if length is None else f.read(length)
        except FileNotFoundError:
            return None

    def swap(self, key, expected_etag, data):
        """Replace key only if its etag still matches. Returns True on success."""
        path = self._path(key)
        with open(self.root / ".swap-lock", "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            current = self.get(key)
            if current is None or etag(current) != expected_etag:
                return False
            os.replace(self._temp(data), path)
            return True

    def list(self, prefix):
        base = self._path(prefix)
        if not base.exists():
            return []
        return sorted(str(p.relative_to(self.root)) for p in base.rglob("*")
                      if p.is_file() and not p.name.startswith(".tmp-"))

    def modified(self, key):
        """Last-modified time in seconds, or None if missing."""
        try:
            return self._path(key).stat().st_mtime
        except FileNotFoundError:
            return None

    def delete(self, key):
        self._path(key).unlink(missing_ok=True)
