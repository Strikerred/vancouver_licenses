"""
Casts the raw string payloads into the typed schema from configs.

Everything here is driven off COLUMNS, so adding a field is a config change.

The rule I follow: a value that will not cast becomes NA and gets recorded, it
never becomes a guess and it never silently becomes zero. pandas will happily
turn a bad number into NaN and a bad date into NaT, which is the behaviour I
want, but only if the column is a nullable dtype. A plain int64 column cannot
hold NA, so a missing employee count would land as 0 and be indistinguishable
from a real zero. That is why the config uses Int64 and not int64.
"""

from __future__ import annotations

import logging

import pandas as pd

from transformer.configs import business_licences_configs as cfg

log = logging.getLogger(__name__)


def rename_and_select(frame: pd.DataFrame) -> pd.DataFrame:
    """Raw names to curated names, and drop anything not in the schema."""
    present = {raw: name for raw, (name, _) in cfg.COLUMNS.items() if raw in frame.columns}
    missing = [raw for raw in cfg.COLUMNS if raw not in frame.columns]
    if missing:
        log.warning("raw columns absent from this extract: %s", missing)

    out = frame[list(present)].rename(columns=present)
    for geo in cfg.GEO_SOURCE_COLUMNS:
        if geo in frame.columns:
            out[geo] = frame[geo]
    return out


def cast_types(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """
    Apply the schema. Returns the frame plus a count of values lost per column.

    The loss count is the important part. If 40 licence numbers failed to parse
    as integers I want that on the screen, not discovered in a dashboard three
    weeks later.

    Timestamps are parsed with utc=True because issueddate and extractdate both
    carry a +00:00 offset. Without it pandas hands back an object column of mixed
    timezone awareness, and then every later comparison is a coin flip: comparing
    an aware timestamp to a naive one either raises or silently does something you
    did not mean, depending on the pandas version.

    folder_year arrives as the two digit '24'. It gets turned into 2024 so the
    curated layer holds a real year and nobody querying it has to know the
    portal's encoding.
    """
    out = frame.copy()
    losses: dict[str, int] = {}

    for raw_name, (name, dtype) in cfg.COLUMNS.items():
        if name not in out.columns:
            continue

        before = out[name].notna().sum()

        if dtype == "timestamp":
            out[name] = pd.to_datetime(out[name], errors="coerce", utc=True, format="ISO8601")
        elif dtype == "date":
            out[name] = pd.to_datetime(out[name], errors="coerce", format="ISO8601").dt.date
        elif dtype in ("Int64", "float64"):
            out[name] = pd.to_numeric(out[name], errors="coerce")
            if dtype == "Int64":
                out[name] = out[name].round().astype("Int64")
        else:
            out[name] = out[name].astype("string")

        after = out[name].notna().sum()
        if after < before:
            losses[name] = int(before - after)

    for name in cfg.TWO_DIGIT_YEAR_COLUMNS:
        if name in out.columns:
            two_digit = out[name]
            out[name] = (two_digit + 2000).where(two_digit <= 79, two_digit + 1900)

    for name, lost in losses.items():
        log.warning("cast %s: %d value(s) would not parse, set to NA", name, lost)

    return out, losses
