"""
Turns the raw json the downloader writes into a typed, checked parquet layer.

    python transform.py                                   # every year available
    python transform.py --year 2026                       # one year
    python transform.py --since 2026-08-17 --until 2026-08-23   # one week
    python transform.py --dry-run                         # run the checks, write nothing

The stages, each its own module under transformer/:

    combiner/partition_combiner combine the year partitions into one frame
    reader/json_reader          read one year of raw json
    manipulator/type_caster     raw strings into the typed schema
    manipulator/text_standardiser  whitespace, casing, postal codes
    manipulator/geo_parser      nested geojson into latitude and longitude
    util/quality                data quality checks and primary key enforcement
    exporter/parquet_exporter   parquet out, plus quarantine

configs/business_licences_configs.py holds the schema, the key, the tie break
order and the check thresholds, so most changes are config and not code.

What this layer guarantees
--------------------------
    licence_rsn is a real int64 and it is unique. Enforced, not assumed, and the
    transform refuses to write if it is not.

    issued_date and extract_date are UTC timestamps, expired_date is a date.
    Anything that would not parse is NA and the count is logged, never a guess
    and never a silent zero.

    latitude and longitude are plain floats, pulled out of the nested geojson.
    GeoJSON stores [longitude, latitude] in that order, which is the easiest
    thing in this whole task to get backwards without any error being raised.

    Every row carries dq_flags saying what is odd about it, and rows that are not
    usable records go to quarantine.parquet rather than being deleted.

The date window is applied at the END, after the primary key is enforced, not in
the reader. Filtering first hides duplicate keys whose two rows fall on different
days: I found 4 duplicates in the full 2026 partition but only 3 when I filtered
to a week first, because the fourth pair straddled the boundary. So the whole
partition gets cast and deduplicated, and the window only decides what gets
written.

Output is one parquet file per issued date under <year>/<month>/, so a daily run
writes one file and re-running a day replaces exactly that file.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from transformer.data_transformer import transform
from transformer.reader.json_reader import available_years

log = logging.getLogger("transform")

PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_CITY = "vancouver"
DEFAULT_DATASET = "business-licences"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Transform raw business licence json into typed parquet."
    )
    parser.add_argument("--city", default=DEFAULT_CITY)
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--year", action="append", dest="years", type=int,
                        metavar="YEAR", help="only these year(s), e.g. 2026")
    parser.add_argument("--since", metavar="YYYY-MM-DD",
                        help="only rows issued on or after this date")
    parser.add_argument("--until", metavar="YYYY-MM-DD",
                        help="only rows issued on or before this date")
    parser.add_argument("--include-undated", action="store_true",
                        help="with a date window, also write the rows that have "
                             "no issued date (Pending licences)")
    parser.add_argument("--raw-root", default=None, help="override the raw json root")
    parser.add_argument("--curated-root", default=None, help="override the parquet root")
    parser.add_argument("--dry-run", action="store_true",
                        help="run everything including the checks, but write nothing")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(message)s",
    )

    raw_root = Path(args.raw_root) if args.raw_root else (
        PROJECT_ROOT / "data" / args.city / args.dataset)
    curated_root = Path(args.curated_root) if args.curated_root else (
        PROJECT_ROOT / "curated")

    years = args.years or available_years(raw_root)
    if not years:
        log.error("no raw partitions under %s, run pipeline.py first", raw_root)
        return 1

    missing = [y for y in years if not (raw_root / str(y) / "records.json").exists()]
    if missing:
        log.error("no raw data for year(s) %s; available: %s",
                  missing, available_years(raw_root))
        return 2

    now = pd.Timestamp(datetime.now(timezone.utc))
    log.info("transforming %s/%s year(s) %s, now=%s",
             args.city, args.dataset, years, now.isoformat())

    try:
        summary = transform(
            raw_root=raw_root, curated_root=curated_root,
            city=args.city, dataset=args.dataset, years=years, now=now,
            since=args.since, until=args.until,
            include_undated=args.include_undated, dry_run=args.dry_run,
        )
    except Exception as exc:                           # noqa: BLE001 - boundary
        log.error("transform FAILED: %s", exc)
        return 1

    log.info("curated=%d rejected=%d flagged=%d cols=%d",
             summary["curated_rows"], summary["rejected_rows"],
             summary["flagged_rows"], summary.get("columns", 0))
    if summary["flags"]:
        log.info("  flags on written rows: %s", summary["flags"])
    if summary.get("flags_before_window"):
        log.info("  flags before the window: %s", summary["flags_before_window"])
    if summary["reject_reasons"]:
        log.warning("  rejects: %s", summary["reject_reasons"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
