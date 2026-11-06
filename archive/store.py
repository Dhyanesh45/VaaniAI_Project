"""A local folder that behaves like the parts of S3 we rely on.

put_if_absent mirrors PutObject with `If-None-Match: *`.
swap mirrors PutObject with `If-Match: <etag>`.
Only this file would change to run against real object storage.
"""
import fcntl
import hashlib
import os
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

    def put_if_absent(self, key, data):
        """Create key. Returns False (S3's 412) if it already exists."""
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = self._temp(data)
        try:
            os.link(temp, path)  # link is atomic and fails if path exists
            return True
        except FileExistsError:
            return False
        finally:
            os.unlink(temp)

    def get(self, key):
        try:
            return self._path(key).read_bytes()
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
        return sorted(f"{prefix}/{p.name}" for p in base.iterdir())

    def delete(self, key):
        self._path(key).unlink(missing_ok=True)
