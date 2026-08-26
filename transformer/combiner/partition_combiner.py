"""
Combines the raw year partitions into one frame before anything else happens.

I did not think this stage was needed. One dataset, one portal, so combining
looked like a concat with a nice name. I was wrong, and the reason is worth
writing down because it is not obvious.

The raw layer is partitioned by folderyear, which comes from the licence number.
The curated layer is partitioned by issued_date, because that is the business
date people filter on. Those two do not line up:

    folder year 2024 -> issued dates 2023-11-04 .. 2025-10-06
    folder year 2025 -> issued dates 2024-11-03 .. 2026-04-21
    folder year 2026 -> issued dates 2025-01-01 .. 2026-10-01

A licence for 2026 can be issued in November 2025, which makes sense, you renew
early. So folder years overlap on the calendar: 82 shared days between 2024 and
2025, and 69 between 2025 and 2026.

That means transforming one folder year at a time and writing files named by
issued date is data loss. Transform 2025, it writes 2025/11/2025-11-04.parquet.
Then transform 2026, and it writes the same path with only its own rows. The
2025 rows for that day are gone. No error, no warning, the file is valid parquet
and the row count looks plausible. Same shape as every other bug in this dataset:
the pipeline is green and the answer is wrong.

So everything gets combined first, deduplicated across the whole set, and only
then split by date. It also means the primary key is enforced globally rather
than per partition, which is more correct anyway: licence_rsn identifies a
licence, not a licence within a year.

Cost is that a run always reads every partition, about 205k rows and fifteen
seconds. Worth it.
"""

from __future__ import annotations

import logging

import pandas as pd

from transformer.reader import json_reader

log = logging.getLogger(__name__)


def combine_years(raw_root, years: list[int]) -> pd.DataFrame:
    """Read the given year partitions and stack them into one frame."""
    frames = []
    for year in years:
        frame = json_reader.read_partition(raw_root, year)
        if frame.empty:
            log.warning("folder year %s is empty, skipping", year)
            continue
        frames.append(frame)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    log.info("combined %d folder year(s) into %d rows", len(frames), len(combined))
    return combined
