# Archive prototype, step 1: the commit rule

    pip install pytest
    python -m pytest -q

Write `LogWriter.commit` in `archive/log.py`, following the numbered steps in its
docstring, until every test passes.

`archive/store.py` is a local folder that imitates S3 conditional writes. It has not
been run against real S3, so results here show the logic, not S3 behaviour.
