"""Shared test helpers.

Every test asks for storage through `new_store`. By default that is a local
folder. Set ARCHIVE_S3_ENDPOINT (see run.sh s3) and the same tests run against
a real S3-compatible server instead.
"""
import os
import uuid

import pytest

from archive.store import LocalStore

_bucket_ready = []


@pytest.fixture
def new_store(tmp_path):
    endpoint = os.environ.get("ARCHIVE_S3_ENDPOINT")
    run = uuid.uuid4().hex[:10]

    def make(name="store"):
        if endpoint:
            from archive.s3store import S3Store
            s = S3Store(os.environ.get("ARCHIVE_S3_BUCKET", "archive-test"),
                        prefix=f"{run}/{name}/", endpoint=endpoint)
            if not _bucket_ready:
                s.ensure_bucket()
                _bucket_ready.append(True)
            return s
        return LocalStore(tmp_path / name)

    return make


class InterruptingStore:
    """Runs another machine's work just before one chosen storage call."""

    def __init__(self, inner, fire_at, action):
        self.inner, self.fire_at, self.action = inner, fire_at, action
        self.calls = 0

    def __getattr__(self, name):
        real = getattr(self.inner, name)

        def wrapped(*args, **kwargs):
            self.calls += 1
            if self.calls == self.fire_at:
                self.action()
            return real(*args, **kwargs)

        return wrapped


class CountingStore:
    """Counts storage calls by method name (to prove how many GETs a read costs)."""

    def __init__(self, inner):
        self.inner, self.counts = inner, {}

    def __getattr__(self, name):
        real = getattr(self.inner, name)

        def wrapped(*args, **kwargs):
            self.counts[name] = self.counts.get(name, 0) + 1
            return real(*args, **kwargs)

        return wrapped
