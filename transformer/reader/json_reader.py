"""
Reads the raw json the downloader wrote into a DataFrame.

Takes a year so we can transform one partition at a time.

Note there is deliberately NO date filter here. It used to be in this file, which
is the obvious place for it, and it was wrong: filtering before deduplication
hides duplicate primary keys whose two rows fall on different dates. The window
lives at the end of data_transformer instead, after the key is enforced. See the
docstring there.

Nothing is cast here either. The reader's job is to get the records into a frame
with the raw column names intact, so if a cast blows up later you can still see
exactly what came in.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)


def available_years(raw_root: Path) -> list[int]:
    """Year folders the downloader has written, sorted."""
    if not raw_root.exists():
        return []
    years = []
    for child in raw_root.iterdir():
        if child.is_dir() and child.name.isdigit() and (child / "records.json").exists():
            years.append(int(child.name))
    return sorted(years)


def read_partition(raw_root: Path, year: int) -> pd.DataFrame:
    """One year of raw records, column names untouched."""
    path = raw_root / str(year) / "records.json"
    if not path.exists():
        raise FileNotFoundError(f"no raw file for {year}: {path}")

    with path.open(encoding="utf-8") as stream:
        records = json.load(stream)

    log.info("read %d raw records from %s", len(records), path)
    return pd.DataFrame.from_records(records)
