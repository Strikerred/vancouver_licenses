"""
Data quality checks, primary key enforcement, and the reject/flag split.

Two severities, because "invalid" covers two different situations and treating
them the same is how you either lose data or ship rubbish.

    REJECT  the row is not a usable record. Null or non numeric primary key, or
            it lost a primary key tie break. These go to quarantine as parquet,
            with a reason, and stay out of the curated table.

    FLAG    the row is fine but something is worth knowing. Future dated,
            unknown status, expired before issued, bad postal code, impossible
            coordinates. Stays in the curated table with the reason listed in
            dq_flags.

Why flag and not drop for most of it: dropping is destructive and it hides the
problem. There are 18 future dated licences in 2026, all status Issued and dated
the 1st of a coming month, so they are almost certainly scheduled issuances
rather than corruption. If I dropped them, the curated count would disagree with
the source and somebody would lose an afternoon to it. Flagged, they are visible
and still queryable, and anyone who wants only currently effective licences can
filter.

Quarantine goes to parquet next to the data rather than to a log line, because a
log gets rotated away and a parquet file can be queried a month later when
somebody asks what happened to that licence.
"""

from __future__ import annotations

import logging

import pandas as pd

from transformer.configs import business_licences_configs as cfg

log = logging.getLogger(__name__)

FLAG_COLUMN = "dq_flags"
REJECT_COLUMN = "dq_reject_reason"


def _add_flag(flags: pd.Series, mask: pd.Series, label: str) -> pd.Series:
    mask = mask.fillna(False)
    return flags.mask(mask, flags.where(flags.eq(""), flags + ";").fillna("") + label)


def run_checks(frame: pd.DataFrame, now: pd.Timestamp) -> tuple[pd.DataFrame, dict[str, int]]:
    """
    Apply the soft checks and write the results into a dq_flags column.

    `now` is passed in rather than read from the clock here so a run is
    reproducible and testable. A check that depends on a hidden clock cannot be
    unit tested and gives different answers depending on when it runs.
    """
    out = frame.copy()
    flags = pd.Series("", index=out.index, dtype="string")
    counts: dict[str, int] = {}

    def record(mask: pd.Series, label: str) -> None:
        nonlocal flags
        mask = mask.fillna(False)
        hit = int(mask.sum())
        if hit:
            counts[label] = hit
            flags = _add_flag(flags, mask, label)

    if "issued_date" in out.columns:
        record(out["issued_date"].notna() & (out["issued_date"] > now),
               "future_issued_date")

    if {"issued_date", "expired_date"} <= set(out.columns):
        expired = pd.to_datetime(out["expired_date"], errors="coerce", utc=True)
        record(out["issued_date"].notna() & expired.notna()
               & (expired < out["issued_date"].dt.floor("D")),
               "expired_before_issued")

    if "status" in out.columns:
        record(out["status"].notna() & ~out["status"].isin(cfg.VALID_STATUSES),
               "unknown_status")

    if "postal_code_valid" in out.columns:
        record(out["postal_code_valid"].eq(False), "postal_code_not_canadian")

    if {"latitude", "longitude"} <= set(out.columns):
        has_point = out["latitude"].notna() & out["longitude"].notna()
        lat_lo, lat_hi = cfg.LATITUDE_BOUNDS
        lon_lo, lon_hi = cfg.LONGITUDE_BOUNDS
        record(has_point & ~(out["latitude"].between(lat_lo, lat_hi)
                             & out["longitude"].between(lon_lo, lon_hi)),
               "coordinates_out_of_bounds")

    for name in cfg.REQUIRED_NOT_NULL:
        if name in out.columns and name != cfg.PRIMARY_KEY:
            record(out[name].isna(), f"null_{name}")

    out[FLAG_COLUMN] = flags.replace("", pd.NA)
    return out, counts


def enforce_primary_key(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Make the primary key unique. Returns (kept, rejected).

    Two ways a row gets rejected here:

      1. The key is null. It failed to cast to a number or it was never there.
         Without an identity the row cannot be joined, updated or deduplicated,
         so it is not a record.

      2. It lost a tie break. The publisher documents licence_rsn as unique and
         it is, in the closed years. In the live partition a handful of rows
         break it, and they are not duplicates: they are two versions of the same
         licence caught mid update, differing in name or address. One of them has
         to win.

    The tie break is DEDUPE_ORDER from configs: most complete row first (fewest
    nulls, so we keep the version that actually has the address), then newest
    extract, then highest revision. The last key is only there to make it
    deterministic. Two runs over the same input must produce the same output, and
    a sort that leaves ties unresolved does not guarantee that.
    """
    key = cfg.PRIMARY_KEY
    out = frame.copy()

    null_key = out[key].isna()
    rejected_null = out.loc[null_key].copy()
    if len(rejected_null):
        rejected_null[REJECT_COLUMN] = f"null_{key}"
        log.warning("%d row(s) rejected: %s is null", len(rejected_null), key)
    out = out.loc[~null_key]

    duplicated = out[key].duplicated(keep=False)
    if not duplicated.any():
        return out, rejected_null

    out["_null_count"] = out.isna().sum(axis=1)
    sort_columns = [c for c, _ in cfg.DEDUPE_ORDER if c in out.columns]
    ascending = [asc for c, asc in cfg.DEDUPE_ORDER if c in out.columns]

    ordered = out.sort_values(sort_columns, ascending=ascending, kind="mergesort")
    winner = ~ordered[key].duplicated(keep="first")

    kept = ordered.loc[winner].drop(columns="_null_count")
    losers = ordered.loc[~winner].drop(columns="_null_count").copy()
    losers[REJECT_COLUMN] = f"duplicate_{key}_lost_tiebreak"

    log.warning("%d duplicate %s row(s) rejected after tie break, %d kept",
                len(losers), key, len(kept))
    log.warning("  affected keys: %s",
                sorted(losers[key].astype("Int64").dropna().unique().tolist())[:10])

    rejected = pd.concat([rejected_null, losers], ignore_index=True)
    return kept.sort_index(), rejected


def summarise(frame: pd.DataFrame, rejected: pd.DataFrame) -> dict[str, object]:
    """
    One dict describing what actually got written, for the log line at the end.

    The flag counts are recomputed from the frame rather than taken from
    run_checks. run_checks sees the whole partition, but the date window narrows
    what we write afterwards, so its counts would describe rows that are not in
    the output. Reporting 47 bad postal codes next to 2 flagged rows is the kind
    of summary that makes people stop trusting the summary.
    """
    flags: dict[str, int] = {}
    if FLAG_COLUMN in frame.columns and len(frame):
        exploded = frame[FLAG_COLUMN].dropna().str.split(";").explode()
        flags = {str(k): int(v) for k, v in exploded.value_counts().items()}

    return {
        "curated_rows": int(len(frame)),
        "rejected_rows": int(len(rejected)),
        "flagged_rows": int(frame[FLAG_COLUMN].notna().sum())
                        if FLAG_COLUMN in frame.columns else 0,
        "flags": dict(sorted(flags.items(), key=lambda kv: -kv[1])),
        "reject_reasons": (rejected[REJECT_COLUMN].value_counts().to_dict()
                           if REJECT_COLUMN in rejected.columns and len(rejected) else {}),
    }
