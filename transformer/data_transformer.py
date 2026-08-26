"""
Runs the stages in order over every raw partition at once.

    combine -> cast -> standardise text -> parse geo -> checks -> primary key
            -> date window -> export by issued date

Each stage is its own module and each one takes a frame and gives back a frame,
so you can run them one at a time in a notebook when something looks wrong. That
is most of why they are split at all.

Why everything is combined first
--------------------------------
The raw layer is partitioned by folderyear, which comes off the licence number.
The curated layer is partitioned by issued_date, which is the date people
actually filter on. Those do not line up, because a 2026 licence can be issued in
November 2025. Folder years overlap on the calendar by 82 days between 2024 and
2025, and 69 days between 2025 and 2026.

So transforming one folder year at a time and writing files named by issued date
loses data: the second year writes the same day file and the first year's rows for
that day are gone, with no error and a plausible row count. Combining first fixes
it, and it also makes the primary key global, which is what licence_rsn actually
is. See combiner/partition_combiner.py.

Why the date window is applied LAST
-----------------------------------
Same class of problem. My first version filtered by date in the reader, which is
the obvious place: read less, work less. It hides duplicate keys whose two rows
sit on different dates. The full 2026 partition has 4 duplicate licence_rsn
values, but a one week window only found 3, because the fourth pair straddled the
boundary. So uniqueness is enforced across everything and the window only decides
what gets written.

Undated rows work the same way. 11,874 rows have no issued_date because the
licence is Pending. They go through every stage and land under <year>/unknown/,
but a windowed run does not rewrite them every day unless you ask for them.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from transformer.combiner import partition_combiner
from transformer.configs import business_licences_configs as cfg
from transformer.exporter import parquet_exporter
from transformer.manipulator import geo_parser, text_standardiser, type_caster
from transformer.util import quality

log = logging.getLogger(__name__)


def _apply_window(
    frame: pd.DataFrame,
    since: str | None,
    until: str | None,
    include_undated: bool,
) -> pd.DataFrame:
    """
    Narrow to an issued_date window, inclusive on both ends, on the typed column.

    Undated rows are excluded when a window is given, because a Pending licence
    with no issued date does not belong to any particular week and rewriting all
    of them on every daily run is pointless. Pass include_undated to keep them. A
    run with no window keeps everything.

    Either way the excluded count is logged. Silence is what makes this kind of
    filter dangerous.
    """
    if not since and not until:
        return frame

    issued = frame["issued_date"]
    window = pd.Series(True, index=frame.index)
    if since:
        window &= issued >= pd.Timestamp(since, tz="UTC")
    if until:
        window &= issued < pd.Timestamp(until, tz="UTC") + pd.Timedelta(days=1)
    window = window.fillna(False) & issued.notna()

    undated = issued.isna()
    if include_undated:
        window |= undated

    out = frame.loc[window]
    log.info("window %s..%s kept %d of %d rows (%d undated %s)",
             since or "-", until or "-", len(out), len(frame), int(undated.sum()),
             "included" if include_undated else "excluded")
    return out


def transform(
    raw_root: Path,
    curated_root: Path,
    city: str,
    dataset: str,
    years: list[int],
    now: pd.Timestamp,
    since: str | None = None,
    until: str | None = None,
    include_undated: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Everything through the whole chain. Returns a summary for the caller to log.

    `now` comes in from outside so the future date check is reproducible instead
    of depending on when the job happened to run.
    """
    raw = partition_combiner.combine_years(raw_root, years)
    if raw.empty:
        log.warning("no raw rows for years %s, nothing to do", years)
        return {"years": years, "curated_rows": 0, "rejected_rows": 0,
                "flagged_rows": 0, "flags": {}, "reject_reasons": {}}

    frame = type_caster.rename_and_select(raw)
    frame, cast_losses = type_caster.cast_types(frame)

    frame = text_standardiser.standardise_text(frame)
    frame, invalid_postal = text_standardiser.standardise_postal_codes(frame)

    frame, geo_counts = geo_parser.parse_coordinates(frame)

    frame, flag_counts = quality.run_checks(frame, now=now)

    frame, rejected = quality.enforce_primary_key(frame)

    duplicates = int(frame[cfg.PRIMARY_KEY].duplicated().sum())
    if duplicates:
        raise AssertionError(
            f"{cfg.PRIMARY_KEY} still has {duplicates} duplicate(s) after "
            "enforcement, refusing to write"
        )

    frame = _apply_window(frame, since, until, include_undated)

    if len(rejected):
        log.info("quarantine covers everything read: %d row(s)", len(rejected))

    summary: dict[str, Any] = {
        "years": years,
        **quality.summarise(frame, rejected),
        "flags_before_window": dict(sorted(flag_counts.items(), key=lambda kv: -kv[1])),
        "cast_losses": cast_losses,
        "invalid_postal_codes": invalid_postal,
        "coordinates": geo_counts,
        "columns": len(frame.columns),
    }

    if dry_run:
        log.info("dry run, not writing")
        return summary

    summary["written"] = parquet_exporter.write_daily_files(
        frame, rejected, curated_root, city, dataset
    )
    return summary
