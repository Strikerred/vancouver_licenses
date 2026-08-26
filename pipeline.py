"""
Daily batch pipeline that pulls municipal open data into raw JSON files.

    python pipeline.py                                    # download then transform
    python pipeline.py --no-transform                     # download only
    python pipeline.py --city Vancouver --province BritishColumbia
    python pipeline.py --full-refresh                     # re-pull everything
    python pipeline.py --partition 2026                   # just one year
    python pipeline.py --dry-run                          # probe only, no writes
    python pipeline.py --strict                           # duplicate keys fail
    python pipeline.py --with-parquet                     # typed copy as well

This is the end to end job: it downloads, then it transforms. Two stages, two
packages.

    downloader/open_data_configs.py     what to pull    (province / city / dataset)
    downloader/opendatasoft_client.py   how one API works
    downloader/open_data_downloader.py  transport and disk
    transformer/                        raw json -> typed parquet (see transform.py)
    pipeline.py                         this file, runs both stages

The client is separate from the downloader, so a new portal type is a new client
file and one line in the registry. Nothing in the downloader changes.

Stage 2 runs whenever stage 1 downloaded anything, and it transforms EVERY year,
not only the ones that changed. That is not laziness. The raw layer is partitioned
by folder year and the curated layer by issued date, and those overlap: a 2026
licence can be issued in November 2025, so folder years share 151 calendar days
between them. Transform only the changed year and you rewrite those shared day
files with half their rows, no error raised. See combiner/partition_combiner.py.

Run transform.py on its own to rebuild curated without downloading, or to work a
single week.

Why the incremental works at partition level and not row level
--------------------------------------------------------------
This is the main decision in the whole thing, and the source forces it.

extractdate looks like it should be a change token. It is not. This is what it
actually looks like on the Vancouver licences dataset:

    folderyear 24 -> extractdate 2026-08-01T09:34:21 .. 09:34:34   (62,979 rows)
    folderyear 25 -> extractdate 2026-08-01T09:34:22 .. 09:34:35   (69,889 rows)
    folderyear 26 -> extractdate 2026-08-25T07:08:43 .. 07:08:47   (72,237 rows)

Every row inside one folder year has the same extract batch. The few seconds of
spread is just how long their export took to write the rows. So the field tells
you which bulk extract a row came from, not when that row changed. If you query
where extractdate > watermark you get a whole year back or nothing. There is no
row level delta in this API at all.

It also explains their note that says "Daily (for current year extracts)". Only
the current folder year actually refreshes daily. 24 and 25 have not moved since
the 2026-08-01 re-extract.

So the honest version of incremental here is per partition:

    probe every partition, one cheap aggregate row each
    compare that against the file we already have on disk
    only re-pull the ones where the watermark moved or the count moved
    on a normal day that is one download and a few probes

There is no state file and no manifest. The data on disk is the state: I read the
row count and the max extractdate out of the file we wrote last time. One less
thing to keep in sync, and the state can never disagree with the data because it
is the data. The trade off is we parse the existing file to answer "did anything
change", so a second or two per partition instead of milliseconds.

Since there is no manifest, the log is the audit trail, which is why the summary
line reconciles our total against the publisher's own record count.

And when the City re-extracts a closed year, like they did with 24 and 25, the
probe catches it and we re-pull that year. If I had built this as append on
issueddate it would never notice, and we would sit on stale 2024 and 2025
statuses forever.

What this does not do: it cannot give you intra day history. If a licence status
changes twice between two extracts, we only ever see the final one. That is the
source, not the pipeline, but it should be written down somewhere so nobody
assumes otherwise later.

How it fails
------------
    Partitions are independent. One bad partition does not kill the others, the
    run reports what happened and exits non zero so the scheduler sees it.

    A crash means we re-pull, we never skip. There is no watermark to get ahead
    of the data, because the data is the watermark. If the file is not there, or
    it is corrupt and will not parse, we treat the partition as new and download
    it again.

    I raise when the extract cannot be trusted: empty result, rows in the wrong
    partition, a column I do not recognise, a NULL natural key, or the row count
    dropping past shrink_alert_threshold, which means upstream truncated.

    Everything else is a warning in the log. I care about that line. If the
    pipeline shouts about things that are normal, people stop reading it, and
    then it is not telling you anything.

Schedule
--------
Daily. Their current year extract landed around 07:08 UTC the day I built this,
so running about 12:00 UTC gives room for a late publish and we still get same
day data.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from downloader.open_data_configs import PORTALS
from downloader.open_data_downloader import APIDownloader, PartitionProbe
from transformer.data_transformer import transform

log = logging.getLogger("pipeline")

DEFAULT_PROVINCE = "BritishColumbia"
DEFAULT_CITY = "Vancouver"
DEFAULT_DATASET = "business-licences"


@dataclass
class PartitionResult:
    """What happened to one partition on this run."""
    partition: str
    action: str
    row_count: int = 0
    watermark: str | None = None
    path: str | None = None
    warnings: list[str] = field(default_factory=list)
    error: str | None = None


def validate_partition(
    records: list[dict[str, Any]],
    partition: str,
    config: dict[str, Any],
    previous_profile: dict[str, Any] | None,
    label: str | None = None,
) -> tuple[dict[str, Any], list[str]]:
    """
    Check the data before we write it, and build the profile.

    partition is the value the API uses ('24'), because that is what we compare
    the rows against. label is what a person should see ('2024'). Two names for
    the same thing is not great, but the alternative is error messages that talk
    about a folder year nobody typed and no folder on disk is called.

    I only raise when the extract itself cannot be believed. Everything else
    comes back as a warning and gets logged.

    The data quality things I found in this dataset, all measured, not assumed:

    licencersn is what the publisher calls the unique id. It is unique across all
    69,889 rows of folder year 25. It is not unique in folder year 26, where 4
    rows break it. Those are not duplicate rows either, each pair is two
    different versions of the same licence sharing the same key and the same
    revision number, with a different business name or address. Zero of them in
    the closed years and 4 in the one that refreshes daily, so these are records
    caught mid update and they sort themselves out when the year closes. Failing
    the load would throw away 72,233 good rows over 4, and the live year is the
    one people actually want. So I warn, record the keys, and write it as
    received. strict_natural_key makes it fatal, which is what you want when you
    are checking a closed partition.

    (licencenumber, licencerevisionnumber) is not unique either, 11 collisions in
    folder year 25 and 6 in 26. That matches their note about licence numbers
    being reused occasionally. So do not key on the human readable number.

    Null rates get profiled every run but I only warn when they move. Address
    fields are blank for home based businesses, country is 16 to 22% null,
    businessname 6 to 7%. If this warned about a structural 22% every single day
    then nobody would notice the day it becomes 60%.
    """
    label = label or partition
    if not records:
        raise ValueError(f"partition={label}: empty extract, not writing that")

    previous_profile = previous_profile or {}
    frame = pd.DataFrame.from_records(records)
    warnings: list[str] = []

    natural_key = config["natural_key"]
    partition_field = config["partition_field"]
    expected_columns = set(config.get("expected_columns") or ())
    expected_nullable = set(config.get("expected_nullable") or ())

    present = set(frame.columns)
    if expected_columns:
        unexpected = present - expected_columns
        if unexpected:
            raise ValueError(
                f"partition={label}: column(s) I do not know about "
                f"{sorted(unexpected)}. Upstream schema changed, somebody should "
                "look at the downstream contracts before we ingest this"
            )
        absent = expected_columns - present
        if absent:
            warnings.append(f"columns missing from this extract: {sorted(absent)}")

    if natural_key not in present:
        raise ValueError(f"partition={label}: natural key {natural_key} is not there")
    if frame[natural_key].isna().any():
        raise ValueError(f"partition={label}: NULL {natural_key} in the data")

    duplicate_keys: list[str] = []
    duplicate_count = int(frame[natural_key].duplicated().sum())
    if duplicate_count:
        duplicate_keys = sorted(
            str(k) for k in
            frame.loc[frame[natural_key].duplicated(keep=False), natural_key].unique()
        )
        message = (
            f"{duplicate_count} duplicate {natural_key} value(s) "
            f"{duplicate_keys[:10]}. The publisher documents this field as unique "
            "and the live partition says otherwise. Writing it as received, "
            "picking a winner needs a tie break rule and that belongs in transform"
        )
        if config.get("strict_natural_key"):
            raise ValueError(f"partition={label}: {message} [strict]")
        warnings.append(message)

    if {"licencenumber", "licencerevisionnumber"} <= present:
        composite = int(
            frame.duplicated(subset=["licencenumber", "licencerevisionnumber"]).sum()
        )
        if composite:
            warnings.append(
                f"{composite} rows share a (licencenumber, licencerevisionnumber) "
                f"pair. Expected, licence numbers get reused sometimes, "
                f"key on {natural_key}"
            )

    if partition_field in present:
        stray = frame.loc[frame[partition_field].astype(str) != str(partition)]
        if not stray.empty:
            raise ValueError(
                f"partition={label}: {len(stray)} rows have a different "
                f"{partition_field}, so the upstream filter did not hold"
            )

    previous_rows = previous_profile.get("row_count")
    threshold = config.get("shrink_alert_threshold", 0.10)
    if previous_rows:
        delta = (len(frame) - previous_rows) / previous_rows
        if delta < -threshold:
            raise ValueError(
                f"partition={label}: row count dropped {abs(delta):.1%} "
                f"({previous_rows} -> {len(frame)}), past the {threshold:.0%} "
                "tolerance. Treating that as upstream truncation, I am not "
                "overwriting good data with it"
            )

    null_rates = {
        column: round(float(frame[column].isna().mean()), 4)
        for column in frame.columns
    }
    previous_rates = previous_profile.get("null_rates") or {}
    tolerance = config.get("null_rate_drift_tolerance", 0.05)
    for column, rate in null_rates.items():
        if column not in expected_nullable:
            if rate > 0:
                warnings.append(f"nulls in {column} which should not be null: {rate:.2%}")
            continue
        before = previous_rates.get(column)
        if before is not None and abs(rate - before) > tolerance:
            warnings.append(
                f"null rate moved in {column}: {before:.2%} -> {rate:.2%} "
                f"(tolerance {tolerance:.0%})"
            )

    profile: dict[str, Any] = {
        "row_count": int(len(frame)),
        "distinct_natural_keys": int(frame[natural_key].nunique()),
        "duplicate_natural_key_count": duplicate_count,
        "duplicate_natural_keys": duplicate_keys,
        "null_rates": null_rates,
    }
    if "status" in present:
        profile["status_counts"] = {
            str(k): int(v) for k, v in frame["status"].value_counts().items()
        }
    if "issueddate" in present:
        profile["issueddate_null_count"] = int(frame["issueddate"].isna().sum())

    watermark_field = config.get("watermark_field")
    if watermark_field and watermark_field in present:
        values = frame[watermark_field].dropna()
        profile[f"{watermark_field}_min"] = str(values.min()) if not values.empty else None
        profile[f"{watermark_field}_max"] = str(values.max()) if not values.empty else None

    return profile, warnings


def read_existing(downloader: APIDownloader, partition: str) -> dict[str, Any] | None:
    """
    What we already have on disk for this partition, or None if we have nothing.

    There is no separate state file. The data is the state. I read the row count
    and the max watermark straight out of the file we wrote last time and compare
    that against the probe.

    I like this better than a sidecar state file because the two can never
    disagree. With a state file you eventually hit the case where the watermark
    says we have folder year 26 but the file got deleted, or the file is there and
    the state is not, and then you are debugging your own bookkeeping instead of
    the data.

    The cost is that we parse the existing file to answer "did anything change",
    which is a second or two for a 50MB partition instead of a few milliseconds
    for a small state file. Worth it for one less thing that can drift. And since
    we have the old records loaded anyway, we get the previous null rates for free,
    which is what the drift check compares against.
    """
    path = downloader.partition_path(partition)
    if not path.exists():
        return None

    try:
        with path.open(encoding="utf-8") as stream:
            records = json.load(stream)
    except (json.JSONDecodeError, OSError) as exc:
        log.warning("could not read existing %s (%s), treating partition as new",
                    path, exc)
        return None

    if not records:
        return None

    frame = pd.DataFrame.from_records(records)
    watermark_field = downloader.config.get("watermark_field")
    watermark = None
    if watermark_field and watermark_field in frame.columns:
        values = frame[watermark_field].dropna()
        watermark = str(values.max()) if not values.empty else None

    return {
        "row_count": int(len(frame)),
        "watermark": watermark,
        "null_rates": {
            column: round(float(frame[column].isna().mean()), 4)
            for column in frame.columns
        },
    }


def needs_refresh(
    probe: PartitionProbe,
    recorded: dict[str, Any] | None,
) -> tuple[bool, str]:
    """
    Compare what upstream has now against what we already have on disk.

    Two signals, because either one on its own can miss something:

        the watermark moved  -> publisher re-extracted this partition
        the count moved      -> content changed even if the stamp did not
    """
    if recorded is None:
        return True, "nothing on disk yet"
    if probe.watermark and probe.watermark != recorded.get("watermark"):
        return True, f"watermark moved {recorded.get('watermark')} -> {probe.watermark}"
    if probe.row_count != recorded.get("row_count"):
        return True, f"row count moved {recorded.get('row_count')} -> {probe.row_count}"
    return False, "nothing changed upstream"


def process_partition(
    downloader: APIDownloader,
    partition: str,
    full_refresh: bool,
    dry_run: bool,
    with_parquet: bool,
) -> PartitionResult:
    """Probe one partition, and pull it if something moved."""
    label = str(downloader.partition_to_year(partition))

    probe = downloader.probe(partition)
    existing = None if full_refresh else read_existing(downloader, partition)

    refresh, reason = needs_refresh(probe, existing)
    if full_refresh:
        refresh, reason = True, "full refresh asked for"

    if not refresh:
        log.info("partition=%s skipped (%s, %d rows)",
                 label, reason, probe.row_count)
        return PartitionResult(label, "skipped", probe.row_count, probe.watermark)

    log.info("partition=%s pulling (%s)", label, reason)
    if dry_run:
        return PartitionResult(label, "dry-run", probe.row_count, probe.watermark)

    records = downloader.download(partition)
    profile, warnings = validate_partition(
        records, partition, downloader.config,
        previous_profile=existing, label=label,
    )
    for warning in warnings:
        log.warning("partition=%s: %s", label, warning)

    watermark_field = downloader.config.get("watermark_field", "")
    watermark = profile.get(f"{watermark_field}_max") or probe.watermark

    path = downloader.save_records(records, partition)
    if with_parquet:
        downloader.save_dataframe(pd.DataFrame.from_records(records), partition)

    return PartitionResult(label, "written", profile["row_count"],
                           watermark, str(path), warnings)


def run(
    city: str = DEFAULT_CITY,
    province: str = DEFAULT_PROVINCE,
    dataset_id: str = DEFAULT_DATASET,
    partitions: Iterable[str] | None = None,
    full_refresh: bool = False,
    dry_run: bool = False,
    strict: bool = False,
    with_parquet: bool = False,
    output_root: str | None = None,
    run_transform: bool = True,
) -> int:
    """
    Download every partition that moved, then transform.

    A few things in here are worth knowing about.

    The reconciliation in the summary line only runs when the run covered every
    partition. With --partition the total is a subset by definition, so comparing
    it against the publisher's full count reports a mismatch on a perfectly good
    run. I had exactly that happen, and the whole point of that line is that you
    can trust it.

    "Nothing changed upstream" only means curated is current if curated exists. My
    first version skipped stage 2 whenever stage 1 skipped, and cheerfully logged
    that curated was up to date while the directory was empty.

    The parameter is called run_transform and not transform because a parameter
    named transform shadows the imported transform function, and then you get
    "bool object is not callable" from a line that reads perfectly fine.
    """
    started = datetime.now(timezone.utc)
    run_id = started.strftime("%Y-%m-%dT%H%M%SZ")

    overrides: dict[str, Any] = {}
    if strict:
        overrides["strict_natural_key"] = True

    downloader = APIDownloader(city=city, province=province, dataset_id=dataset_id,
                               output_root=output_root, **overrides)
    log.info("%s", downloader)

    metadata = downloader.fetch_metadata()
    log.info("publisher says %s records, data last processed %s",
             metadata.get("records_count"), metadata.get("data_processed"))

    available = downloader.discover_partitions()
    if not available:
        log.error("no partitions found upstream for %s/%s", province, city)
        return 1

    if partitions:
        targets = []
        for value in partitions:
            normalised = downloader.normalise_partition(value)
            if normalised not in available:
                log.error("partition %s is not upstream; available: %s", value,
                          ", ".join(str(downloader.partition_to_year(p))
                                    for p in available))
                return 2
            targets.append(normalised)
    else:
        targets = available

    log.info("partitions to check: %s",
             ", ".join(str(downloader.partition_to_year(p)) for p in targets))

    results: list[PartitionResult] = []

    for partition in targets:
        try:
            results.append(process_partition(
                downloader, partition, full_refresh, dry_run, with_parquet
            ))
        except Exception as exc:                       # noqa: BLE001 - boundary
            label = str(downloader.partition_to_year(partition))
            log.error("partition=%s FAILED: %s", label, exc)
            results.append(PartitionResult(label, "failed", error=str(exc)))

    written = [r for r in results if r.action == "written"]
    failed = [r for r in results if r.action == "failed"]
    ingested = sum(r.row_count for r in written)

    on_disk = sum(r.row_count for r in results if r.action in ("written", "skipped"))
    claimed = metadata.get("records_count")
    if partitions:
        reconciled = f"partial run, {len(targets)} partition(s), not reconciling"
    elif claimed == on_disk:
        reconciled = "match"
    else:
        reconciled = f"MISMATCH vs publisher {claimed}"
    log.info("download done in %.1fs: %d written, %d skipped, %d failed, "
             "%d rows this run, %d rows total (%s)",
             (datetime.now(timezone.utc) - started).total_seconds(),
             len(written), sum(1 for r in results if r.action == "skipped"),
             len(failed), ingested, on_disk, reconciled)
    for result in failed:
        log.error("failed partition %s: %s", result.partition, result.error)

    if not run_transform or dry_run:
        if run_transform and dry_run:
            log.info("dry run, skipping transform")
        return 1 if failed else 0

    curated_root = Path(downloader.config["output_root"]).parent / "curated"
    raw_root = Path(downloader.config["output_root"]) / city.lower() / dataset_id
    now = pd.Timestamp(datetime.now(timezone.utc))

    curated_present = any(curated_root.rglob("*.parquet")) if curated_root.exists() else False
    if not written and curated_present:
        log.info("nothing re-downloaded and curated is present, skipping transform")
        return 1 if failed else 0
    if not written:
        log.info("nothing re-downloaded but curated is empty, building it")

    all_years = [int(r.partition) for r in results
                 if r.action in ("written", "skipped")]
    log.info("transforming year(s) %s (%s changed)",
             all_years, ", ".join(r.partition for r in written))

    try:
        summary = transform(
            raw_root=raw_root, curated_root=curated_root,
            city=city.lower(), dataset=dataset_id, years=sorted(all_years), now=now,
        )
    except Exception as exc:                           # noqa: BLE001 - boundary
        log.error("transform FAILED: %s", exc)
        return 1

    log.info("transform: curated=%d rejected=%d flagged=%d",
             summary["curated_rows"], summary["rejected_rows"],
             summary["flagged_rows"])
    if summary["flags"]:
        log.info("  flags: %s", summary["flags"])
    if summary["reject_reasons"]:
        log.warning("  rejects: %s", summary["reject_reasons"])

    return 1 if failed else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Pull municipal open data records into partitioned raw JSON."
    )
    parser.add_argument("--city", default=DEFAULT_CITY)
    parser.add_argument("--province", default=DEFAULT_PROVINCE)
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--partition", action="append", dest="partitions",
                        metavar="YEAR", help="only these year(s), e.g. 2026")
    parser.add_argument("--full-refresh", action="store_true",
                        help="re-pull every partition and ignore the watermarks")
    parser.add_argument("--dry-run", action="store_true",
                        help="probe upstream and report, do not write anything")
    parser.add_argument("--strict", action="store_true",
                        help="duplicate natural keys become fatal, for closed partitions")
    parser.add_argument("--with-parquet", action="store_true",
                        help="also write a typed parquet copy next to the raw json")
    parser.add_argument("--output-root", default=None,
                        help="override where the data lands")
    parser.add_argument("--no-transform", action="store_true",
                        help="download only, skip the transform stage")
    parser.add_argument("--list-cities", action="store_true",
                        help="print the province/city pairs we have config for")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(message)s",
    )

    if args.list_cities:
        for province, cities in PORTALS.items():
            for city in cities:
                print(f"{province}/{city}")
        return 0

    try:
        return run(
            city=args.city, province=args.province, dataset_id=args.dataset,
            partitions=args.partitions, full_refresh=args.full_refresh,
            dry_run=args.dry_run, strict=args.strict,
            with_parquet=args.with_parquet, output_root=args.output_root,
            run_transform=not args.no_transform,
        )
    except ValueError as exc:
        log.error("config problem: %s", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
