"""
Writes the curated frame out as parquet, one file per issued date.

    curated/<city>/<dataset>/<year>/<month>/<YYYY-MM-DD>.parquet
    curated/<city>/<dataset>/<year>/<month>/<YYYY-MM-DD>.quarantine.parquet
    curated/<city>/<dataset>/<year>/unknown/no-issued-date.parquet

Year and month come from issued_date, because that is the business date people
filter on. A daily run writes one file, a backfill writes one per day it covers,
and re-running a day replaces exactly that file and touches nothing else. That
last part is the real reason for this layout: the raw layer overwrites a whole
year at a time, and here a bad day costs one file instead of the year.

Parquet rather than json because it carries the schema. That is the point of this
whole layer: raw is untyped strings, and if the output were json again every
reader would have to re-guess the types and they would not all guess the same.

Rows with no issued_date go to <folder_year>/unknown/. There are 11,877 of them
in 2026, they are Pending licences that were never issued, and they are real
records. Putting them in a named folder keeps them queryable and obvious. The
alternative, quietly dropping anything that cannot be placed on a calendar, is
the same bug the downloader avoids by partitioning on folderyear.

Quarantine sits next to the day it came from rather than in its own tree, so
anyone looking at a day's output trips over the rejects instead of having to know
they exist.
"""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

UNKNOWN_DATE_DIR = "unknown"
UNKNOWN_DATE_FILE = "no-issued-date"


def _atomic_to_parquet(frame: pd.DataFrame, target: Path) -> None:
    """Temp file then os.replace, so a reader never sees a half written file."""
    target.parent.mkdir(parents=True, exist_ok=True)
    handle, tmp_name = tempfile.mkstemp(dir=str(target.parent), suffix=".tmp")
    os.close(handle)
    try:
        frame.to_parquet(tmp_name, index=False)
        os.replace(tmp_name, target)
    except BaseException:
        Path(tmp_name).unlink(missing_ok=True)
        raise


def _day_keys(frame: pd.DataFrame) -> pd.DataFrame:
    """
    Work out the year / month / file name for every row.

    Falls back to folder_year with an 'unknown' month when issued_date is null,
    so those rows still land somewhere deterministic instead of being skipped.
    """
    issued = pd.to_datetime(frame.get("issued_date"), errors="coerce", utc=True)
    undated = issued.isna()

    keys = pd.DataFrame(index=frame.index)
    keys["year"] = issued.dt.year.astype("Int64")
    keys["month"] = issued.dt.strftime("%m")
    keys["name"] = issued.dt.strftime("%Y-%m-%d")

    if undated.any():
        if "folder_year" in frame.columns:
            keys.loc[undated, "year"] = frame.loc[undated, "folder_year"]
        keys.loc[undated, "month"] = UNKNOWN_DATE_DIR
        keys.loc[undated, "name"] = UNKNOWN_DATE_FILE

    return keys


def write_daily_files(
    frame: pd.DataFrame,
    rejected: pd.DataFrame,
    curated_root: Path,
    city: str,
    dataset: str,
) -> dict[str, int]:
    """
    Split both frames by issued date and write a parquet file per day.

    Returns counts of what was written so the caller can log it.
    """
    base = Path(curated_root) / city.lower() / dataset
    written = {"curated_files": 0, "curated_rows": 0,
               "quarantine_files": 0, "quarantine_rows": 0}

    if len(frame):
        keys = _day_keys(frame)
        for (year, month, name), group in frame.groupby(
                [keys["year"], keys["month"], keys["name"]], dropna=False):
            if pd.isna(year):
                target = base / UNKNOWN_DATE_DIR / UNKNOWN_DATE_DIR / f"{UNKNOWN_DATE_FILE}.parquet"
            else:
                target = base / str(int(year)) / str(month) / f"{name}.parquet"
            _atomic_to_parquet(group, target)
            written["curated_files"] += 1
            written["curated_rows"] += len(group)
            log.debug("wrote %d rows -> %s", len(group), target)

    if len(rejected):
        keys = _day_keys(rejected)
        for (year, month, name), group in rejected.groupby(
                [keys["year"], keys["month"], keys["name"]], dropna=False):
            if pd.isna(year):
                target = (base / UNKNOWN_DATE_DIR / UNKNOWN_DATE_DIR
                          / f"{UNKNOWN_DATE_FILE}.quarantine.parquet")
            else:
                target = base / str(int(year)) / str(month) / f"{name}.quarantine.parquet"
            _atomic_to_parquet(group, target)
            written["quarantine_files"] += 1
            written["quarantine_rows"] += len(group)
            log.warning("quarantined %d row(s) -> %s", len(group), target)

    log.info("wrote %d curated file(s) covering %d rows%s",
             written["curated_files"], written["curated_rows"],
             f", plus {written['quarantine_files']} quarantine file(s)"
             if written["quarantine_files"] else "")
    return written
