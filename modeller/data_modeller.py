"""
Builds the dimensional model from the curated parquet and writes it out.

    read curated parquet
      -> business identity
      -> dimensions (business, address, neighbourhood, category, status, date)
      -> fact_licence
      -> derived facts (lifecycle, address change)
      -> temporal (business history, status transitions, renewals)
      -> parquet

The model is a star: fact_licence at the licence record grain in the middle, the
dimensions around it, and three derived tables that pre-compute the expensive
sequence work so the questions do not need a window function over 200k rows every
time somebody asks.

Every table is written as a single parquet file under model/, and every one gets
a key uniqueness assertion before it is written. If a surrogate key is not unique
the build fails rather than shipping a dimension that will fan out a join.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from modeller import dimensions, facts, temporal
from modeller.model_configs import MODEL_TABLES

log = logging.getLogger(__name__)

KEY_COLUMNS = {
    "dim_business": "business_sk",
    "dim_address": "address_sk",
    "dim_neighbourhood": "neighbourhood_sk",
    "dim_category": "category_sk",
    "dim_status": "status_sk",
    "dim_date": "date_sk",
    "fact_licence": "licence_rsn",
    "fact_business_lifecycle": "business_sk",
}


def read_curated(curated_root: Path, city: str, dataset: str) -> pd.DataFrame:
    """
    Read every curated parquet file into one frame.

    Quarantine files are skipped. Those rows were rejected because they are not
    usable records, so putting them into a model that claims a unique business key
    would defeat the point of having rejected them.
    """
    base = Path(curated_root) / city.lower() / dataset
    paths = sorted(p for p in base.rglob("*.parquet")
                   if not p.name.endswith(".quarantine.parquet"))
    if not paths:
        raise FileNotFoundError(f"no curated parquet under {base}, run transform.py first")

    frame = pd.concat([pd.read_parquet(p) for p in paths], ignore_index=True)
    log.info("read %d curated rows from %d file(s)", len(frame), len(paths))
    return frame


def _assert_unique(table: str, frame: pd.DataFrame) -> None:
    key = KEY_COLUMNS.get(table)
    if not key or key not in frame.columns or frame.empty:
        return
    duplicates = int(frame[key].duplicated().sum())
    if duplicates:
        raise AssertionError(
            f"{table}: {key} has {duplicates} duplicate(s), refusing to write a "
            "table whose key would fan out every join to it"
        )


def build_model(
    curated_root: Path,
    model_root: Path,
    city: str,
    dataset: str,
    as_of: pd.Timestamp,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Build every table and write it. Returns row counts for the caller to log.

    as_of is passed in rather than read from the clock, so the activity
    classification is reproducible. Whether a licence counts as expired depends on
    what "now" is, and a model that gives different answers depending on when it
    ran is not one you can test.
    """
    curated = read_curated(curated_root, city, dataset)
    curated = dimensions.business_identity(curated)

    earliest_folder_year = int(pd.to_numeric(curated["folder_year"],
                                             errors="coerce").min())
    log.info("earliest folder year in the data is %d, so lifespan is left "
             "censored at that boundary", earliest_folder_year)

    dim_address, address_sk = dimensions.build_dim_address(curated)
    dim_neighbourhood, neighbourhood_sk = dimensions.build_dim_neighbourhood(curated)
    dim_category, category_sk = dimensions.build_dim_category(curated)
    dim_status, status_sk = dimensions.build_dim_status(curated)

    tables: dict[str, pd.DataFrame] = {
        "dim_business": dimensions.build_dim_business(curated),
        "dim_address": dim_address,
        "dim_neighbourhood": dim_neighbourhood,
        "dim_category": dim_category,
        "dim_status": dim_status,
        "dim_date": dimensions.build_dim_date(curated),
    }

    fact_licence = facts.build_fact_licence(
        curated, address_sk, neighbourhood_sk, category_sk, status_sk, as_of)
    tables["fact_licence"] = fact_licence
    tables["fact_business_lifecycle"] = facts.build_fact_business_lifecycle(
        fact_licence, earliest_folder_year)
    address_change, relocation_counts = facts.build_fact_address_change(
        fact_licence, dim_address)
    tables["fact_address_change"] = address_change
    tables["dim_business_history"] = temporal.build_dim_business_history(
        curated, fact_licence)
    tables["fact_status_transition"] = temporal.build_fact_status_transition(fact_licence)
    tables["fact_renewal"] = temporal.build_fact_renewal(fact_licence)

    for name, frame in tables.items():
        _assert_unique(name, frame)

    missing = [t for t in MODEL_TABLES if t not in tables]
    if missing:
        log.warning("declared tables not built: %s", missing)

    if not dry_run:
        model_root = Path(model_root)
        for name, frame in tables.items():
            target = model_root / f"{name}.parquet"
            target.parent.mkdir(parents=True, exist_ok=True)
            frame.to_parquet(target, index=False)
            log.info("wrote %-28s %7d rows -> %s", name, len(frame), target)

    return {name: len(frame) for name, frame in tables.items()}
