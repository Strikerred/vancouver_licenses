"""
Builds the dimensional model from the curated parquet, and answers the three
municipal questions against it.

    python model.py                                      # build the model
    python model.py --start 2026-01-01 --end 2026-12-31  # date range for Q1
    python model.py --all-closed                         # Q2 without the cohort filter
    python model.py --dry-run                            # build in memory, write nothing

Tables written to model/, one parquet file each:

    dim_business              one business, constructed identity
    dim_address               one distinct physical address
    dim_neighbourhood         one local planning area
    dim_category              one business type / subtype pair
    dim_status                one status, with behaviour flags
    dim_date                  day grain calendar
    dim_business_history      type 2 attribute history, valid_from / valid_to
    fact_licence              one licence record, the base grain
    fact_business_lifecycle   one business, with lifespan
    fact_status_transition    one observed status change
    fact_renewal              one renewal, with the gap
    fact_address_change       one relocation, with distance

Marts written to model/marts/:

    mart_neighbourhood_activity   Q1  active vs cancelled/expired per neighbourhood
    mart_lifespan_by_category     Q2  average lifespan of closed businesses
    mart_address_changes          Q3  businesses that relocated

===============================================================================
THE THREE THINGS THAT DECIDE WHETHER THE ANSWERS ARE RIGHT
===============================================================================

1. There is no business identifier in this data. licence_rsn is a licence record
   and licence_number is YY-NNNNNN, so a renewal next year gets a brand new
   number. Following a business through time needs a constructed key, and it must
   not include the address or a relocation would look like one business closing
   and another opening. See model_configs for the rule and what it costs.

2. Expiry is not closure. These licences are annual, nearly all of them expire on
   31 December, and they get renewed. `expired_date < today` counts every healthy
   business as dead. A licence that expired only means closure if no later licence
   exists for the same business, which is a lookahead, not a row level test.

3. Lifespan is left censored at the start of the dataset. The data begins at
   folder year 2024, so a business licensed since 1998 shows a two year lifespan.
   An average over every closed business measures the observation window as much
   as it measures survival. The default restricts to businesses whose first
   licence is after the earliest year we hold, and both numbers get logged so the
   bias is visible.

Every table asserts its key is unique before it is written. If a surrogate key
collides the build fails, rather than shipping a dimension that fans out every
join to it.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from modeller import analyses
from modeller.data_modeller import build_model, read_curated

log = logging.getLogger("model")

PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_CITY = "vancouver"
DEFAULT_DATASET = "business-licences"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build the business licence dimensional model and answer the "
                    "three municipal questions."
    )
    parser.add_argument("--city", default=DEFAULT_CITY)
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--start", metavar="YYYY-MM-DD",
                        help="start of the range for question 1, default the "
                             "earliest date in the data")
    parser.add_argument("--end", metavar="YYYY-MM-DD",
                        help="end of the range for question 1, default today")
    parser.add_argument("--all-closed", action="store_true",
                        help="question 2 over every closed business instead of the "
                             "fully observed cohort, which is biased by left censoring")
    parser.add_argument("--curated-root", default=None)
    parser.add_argument("--model-root", default=None)
    parser.add_argument("--dry-run", action="store_true",
                        help="build everything in memory, write nothing")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(message)s",
    )

    curated_root = Path(args.curated_root) if args.curated_root else PROJECT_ROOT / "curated"
    model_root = Path(args.model_root) if args.model_root else PROJECT_ROOT / "model"
    as_of = pd.Timestamp(datetime.now(timezone.utc))

    try:
        counts = build_model(curated_root=curated_root, model_root=model_root,
                             city=args.city, dataset=args.dataset,
                             as_of=as_of, dry_run=args.dry_run)
    except FileNotFoundError as exc:
        log.error("%s", exc)
        return 1
    except AssertionError as exc:
        log.error("model integrity check failed: %s", exc)
        return 1

    log.info("model built: %s", counts)

    if args.dry_run:
        log.info("dry run, skipping the marts")
        return 0

    fact_licence = pd.read_parquet(model_root / "fact_licence.parquet")
    lifecycle = pd.read_parquet(model_root / "fact_business_lifecycle.parquet")
    dim_neighbourhood = pd.read_parquet(model_root / "dim_neighbourhood.parquet")
    dim_category = pd.read_parquet(model_root / "dim_category.parquet")
    dim_business = pd.read_parquet(model_root / "dim_business.parquet")
    dim_address = pd.read_parquet(model_root / "dim_address.parquet")
    address_change = pd.read_parquet(model_root / "fact_address_change.parquet")

    start = pd.Timestamp(args.start, tz="UTC") if args.start else \
        fact_licence["issued_date"].min()
    end = pd.Timestamp(args.end, tz="UTC") if args.end else as_of

    marts_root = model_root / "marts"
    marts_root.mkdir(parents=True, exist_ok=True)

    q1 = analyses.active_vs_closed_by_neighbourhood(
        fact_licence, dim_neighbourhood, start, end)
    q1.to_parquet(marts_root / "mart_neighbourhood_activity.parquet", index=False)

    q2 = analyses.lifespan_by_category(
        lifecycle, dim_category, fully_observed_only=not args.all_closed)
    q2.to_parquet(marts_root / "mart_lifespan_by_category.parquet", index=False)

    q3 = analyses.address_changes(address_change, dim_business, dim_address)
    if not q3.empty:
        q3.to_parquet(marts_root / "mart_address_changes.parquet", index=False)

    log.info("marts written to %s", marts_root)
    return 0


if __name__ == "__main__":
    sys.exit(main())
