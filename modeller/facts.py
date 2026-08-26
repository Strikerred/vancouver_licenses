"""
Builds the fact tables.

    fact_licence              one row per licence record, the base grain
    fact_business_lifecycle   one row per business, derived
    fact_address_change       one row per relocation, derived

fact_licence is the only table read straight off the curated layer. The other two
are computed from it, and they exist because the questions they answer would
otherwise need a window function over 200k rows every time somebody asks.

===============================================================================
THE ACTIVITY RULES, WHICH ARE THE WHOLE BALL GAME
===============================================================================
These licences are annual. Nearly all of them expire on 31 December and get
renewed in the new year. So an expired licence tells you almost nothing on its
own: `expired_date < today` counts every healthy renewed business as dead.

A licence that has expired only means the business closed if there is no later
licence for the same business. Which means the lapsed test needs the constructed
business key, and cannot be evaluated one row at a time. That is the part I would
expect a quick implementation to get wrong, and it would get it wrong silently,
producing a closure rate several times too high.

Order matters in the classification, so it is written out rather than left to a
chain of implicit precedence:

    closed_by_action   status is Cancelled or Gone Out of Business
    pending            status is Pending, never issued at all
    inactive           status is Inactive
    lapsed             expired, and NO later licence exists for this business
    superseded         expired, but a later licence does exist, so it was renewed
    active             status is Issued and it has not expired

superseded is the state that has to exist. Without it every expired-but-renewed
licence matches none of the tests and falls through to whatever the default is. I
built it that way first and 54% of the dataset came out looking closed, which is
the failure this docstring was already warning about. An expired licence belonging
to a business that renewed is not a closure, it is last year's paperwork.

Anything still unclassified becomes unknown rather than being folded into a real
state. A default that quietly means something is how the first version went wrong.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from modeller.model_configs import (ACTIVITY_ACTIVE, ACTIVITY_CLOSED,
                                    ACTIVITY_INACTIVE, ACTIVITY_LAPSED,
                                    ACTIVITY_PENDING, ACTIVITY_SUPERSEDED,
                                    ACTIVITY_UNKNOWN, CLOSED_BY_ACTION,
                                    EARTH_RADIUS_KM, STATUS_INACTIVE,
                                    STATUS_ISSUED, STATUS_PENDING, UNKNOWN_KEY)

log = logging.getLogger(__name__)


def _date_sk(series: pd.Series) -> pd.Series:
    """Timestamp column to a YYYYMMDD integer key, with the reserved key for nulls."""
    stamped = pd.to_datetime(series, errors="coerce", utc=True)
    keys = stamped.dt.strftime("%Y%m%d")
    return pd.to_numeric(keys, errors="coerce").fillna(UNKNOWN_KEY).astype("int64")


def build_fact_licence(
    frame: pd.DataFrame,
    address_sk: pd.Series,
    neighbourhood_sk: pd.Series,
    category_sk: pd.Series,
    status_sk: pd.Series,
    as_of: pd.Timestamp,
) -> pd.DataFrame:
    """
    One row per licence record, with foreign keys and the activity classification.

    is_current_revision marks the latest revision of each licence number.
    licence_revision_number runs from 00 up to 10 in this data, and every revision
    is its own row, so any question about how many businesses there are has to
    filter to the current revision or it double counts amended licences.

    has_later_licence is the lookahead that makes the lapsed test possible: for
    each business, is there another licence with a later issue date. Computed once
    here rather than in every downstream query.
    """
    out = pd.DataFrame(index=frame.index)
    out["licence_rsn"] = frame["licence_rsn"]
    out["licence_number"] = frame["licence_number"]
    out["licence_revision_number"] = frame["licence_revision_number"]
    out["business_sk"] = frame["business_sk"].astype("Int64")
    out["address_sk"] = pd.array(address_sk.values, dtype="Int64")
    out["neighbourhood_sk"] = pd.array(neighbourhood_sk.values, dtype="Int64")
    out["category_sk"] = pd.array(category_sk.values, dtype="Int64")
    out["status_sk"] = pd.array(status_sk.values, dtype="Int64")
    out["status"] = frame["status"]
    out["issued_date"] = frame["issued_date"]
    out["expired_date"] = pd.to_datetime(frame["expired_date"], errors="coerce", utc=True)
    out["issued_date_sk"] = _date_sk(frame["issued_date"])
    out["expired_date_sk"] = _date_sk(out["expired_date"])
    out["folder_year"] = frame["folder_year"]
    out["fee_paid"] = frame.get("fee_paid")
    out["number_of_employees"] = frame.get("number_of_employees")
    out["extract_date"] = frame.get("extract_date")

    revision = pd.to_numeric(out["licence_revision_number"], errors="coerce").fillna(-1)
    out["_revision"] = revision
    max_revision = out.groupby("licence_number")["_revision"].transform("max")
    out["is_current_revision"] = out["_revision"].eq(max_revision)

    ordered = out.sort_values(["business_sk", "issued_date"], na_position="first")
    later = (ordered.groupby("business_sk")["issued_date"]
                    .transform(lambda s: s.shift(-1).bfill().notna()))
    out["has_later_licence"] = later.reindex(out.index).fillna(False)

    max_issued = out.groupby("business_sk")["issued_date"].transform("max")
    out["is_latest_licence"] = out["issued_date"].eq(max_issued) | (
        out["issued_date"].isna() & max_issued.isna())

    expired = out["expired_date"].notna() & (out["expired_date"] < as_of)

    activity = pd.Series(pd.NA, index=out.index, dtype="string")
    activity = activity.mask(out["status"].eq(STATUS_ISSUED) & ~expired, ACTIVITY_ACTIVE)
    activity = activity.mask(expired & out["has_later_licence"], ACTIVITY_SUPERSEDED)
    activity = activity.mask(expired & ~out["has_later_licence"], ACTIVITY_LAPSED)
    activity = activity.mask(out["status"].eq(STATUS_INACTIVE), ACTIVITY_INACTIVE)
    activity = activity.mask(out["status"].eq(STATUS_PENDING), ACTIVITY_PENDING)
    activity = activity.mask(out["status"].isin(CLOSED_BY_ACTION), ACTIVITY_CLOSED)
    out["activity_state"] = activity.fillna(ACTIVITY_UNKNOWN)

    out = out.drop(columns="_revision")

    log.info("fact_licence: %d rows, %d current revisions", len(out),
             int(out["is_current_revision"].sum()))
    log.info("  activity: %s", out["activity_state"].value_counts().to_dict())
    return out


def build_fact_business_lifecycle(
    fact_licence: pd.DataFrame,
    earliest_folder_year: int,
) -> pd.DataFrame:
    """
    One row per business, with lifespan.

    HOW LIFESPAN IS APPROXIMATED, AND WHY IT IS A LOWER BOUND. There is no
    closure event in this data. We know a licence's status but not the date the
    status changed, so the best available closure date is the expiry of the
    business's final licence, falling back to its issue date. For a cancellation
    part way through a year that overstates the lifespan by up to a year.

    THE BIGGER PROBLEM IS LEFT CENSORING. The dataset starts at folder year 2024.
    A business licensed continuously since 1998 shows up with a two year
    lifespan, because that is all we can see. Averaging over every closed business
    therefore produces a number that is not a lower bound in any useful sense, it
    is biased toward whatever the observation window happens to be.

    is_fully_observed marks businesses whose first licence is later than the
    earliest folder year in the dataset. Those did not exist in the first year we
    can see, so their start really is their start. That cohort is much smaller,
    and it is the only one an average lifespan should be computed over. The flag
    is on the table rather than filtered out, so the analysis can choose, but the
    honest answer uses it.
    """
    current = fact_licence.loc[fact_licence["is_current_revision"]]
    grouped = current.sort_values("issued_date", na_position="first") \
                     .groupby("business_sk", dropna=False)

    out = pd.DataFrame({
        "business_sk": grouped.size().index,
        "licence_count": grouped["licence_number"].nunique().values,
        "record_count": grouped.size().values,
        "first_issued_date": grouped["issued_date"].min().values,
        "last_issued_date": grouped["issued_date"].max().values,
        "last_expired_date": grouped["expired_date"].max().values,
        "first_folder_year": grouped["folder_year"].min().values,
        "last_folder_year": grouped["folder_year"].max().values,
        "latest_status": grouped["status"].last().values,
        "latest_activity_state": grouped["activity_state"].last().values,
        "latest_category_sk": grouped["category_sk"].last().values,
        "latest_neighbourhood_sk": grouped["neighbourhood_sk"].last().values,
        "latest_address_sk": grouped["address_sk"].last().values,
    })

    out["is_closed"] = out["latest_activity_state"].isin([ACTIVITY_CLOSED, ACTIVITY_LAPSED])
    out["closed_by_action"] = out["latest_activity_state"].eq(ACTIVITY_CLOSED)

    closure = out["last_expired_date"].fillna(out["last_issued_date"])
    out["closure_date"] = closure.where(out["is_closed"])

    lifespan_days = (out["closure_date"] - out["first_issued_date"]).dt.days
    out["lifespan_days"] = lifespan_days
    out["lifespan_years"] = (lifespan_days / 365.25).round(3)

    out["is_fully_observed"] = out["first_folder_year"] > earliest_folder_year

    closed = int(out["is_closed"].sum())
    observed_closed = int((out["is_closed"] & out["is_fully_observed"]).sum())
    log.info("fact_business_lifecycle: %d businesses, %d closed, %d of those "
             "fully observed", len(out), closed, observed_closed)
    if closed:
        log.info("  average lifespan, all closed: %.2f years, fully observed only: %.2f years",
                 out.loc[out["is_closed"], "lifespan_years"].mean(),
                 out.loc[out["is_closed"] & out["is_fully_observed"], "lifespan_years"].mean())
    return out


def _haversine_km(lat1, lon1, lat2, lon2):
    """Great circle distance in km. Vectorised, tolerates NaN by returning NaN."""
    lat1, lon1, lat2, lon2 = (np.radians(np.asarray(v, dtype="float64"))
                              for v in (lat1, lon1, lat2, lon2))
    dlat, dlon = lat2 - lat1, lon2 - lon1
    inner = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(inner))


def build_fact_address_change(
    fact_licence: pd.DataFrame,
    dim_address: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """
    One row per relocation: a business at a different address than the year before.

    COMPARED YEAR OVER YEAR, NOT LICENCE TO LICENCE, and that distinction is the
    difference between a useful answer and nonsense.

    My first version walked consecutive licences per business and flagged every
    address change. It reported 15,096 relocations by 3,619 businesses, over four
    each, with 99.6% of them crossing a neighbourhood boundary. That is not what a
    city's businesses do. What it actually found was multi-location businesses:
    identity is the name, so every branch of the same chain collapses into one
    business, the licences alternate between premises, and each alternation reads
    as a move. The number was plausible enough to publish and completely wrong.

    So this collapses to one row per business per folder year, and only considers
    businesses holding exactly ONE address in both of the years being compared.
    A business with three simultaneous premises is not relocating, it is operating
    three premises, and it is excluded and counted separately rather than quietly
    inflating the answer.

    Unknown addresses are excluded on both sides. A business moving from Unknown
    to a real address is us learning where it is, not the business moving. The cost
    is that home based businesses, withheld since 2018, cannot appear here at all.

    distance_km and crossed_neighbourhood are what make this answer useful. Moving
    200 metres and moving across the city are different economic events, and for a
    municipal question the one that matters is whether the business left the
    neighbourhood.
    """
    usable = fact_licence.loc[fact_licence["is_current_revision"]
                              & fact_licence["address_sk"].ne(UNKNOWN_KEY)]

    per_year = usable.groupby(["business_sk", "folder_year"], as_index=False).agg(
        address_count=("address_sk", "nunique"),
        address_sk=("address_sk", "first"),
        neighbourhood_sk=("neighbourhood_sk", "first"),
        licence_number=("licence_number", "first"),
        year_first_issued=("issued_date", "min"),
    )

    multi_site = per_year["address_count"].gt(1)
    multi_site_businesses = set(per_year.loc[multi_site, "business_sk"])
    single = per_year.loc[~per_year["business_sk"].isin(multi_site_businesses)]

    counts = {
        "businesses_considered": int(single["business_sk"].nunique()),
        "businesses_excluded_multi_site": len(multi_site_businesses),
    }

    ordered = single.sort_values(["business_sk", "folder_year"])
    previous_address = ordered.groupby("business_sk")["address_sk"].shift(1)
    previous_year = ordered.groupby("business_sk")["folder_year"].shift(1)
    previous_area = ordered.groupby("business_sk")["neighbourhood_sk"].shift(1)
    previous_licence = ordered.groupby("business_sk")["licence_number"].shift(1)

    moved = previous_address.notna() & previous_address.ne(ordered["address_sk"])

    out = pd.DataFrame({
        "business_sk": ordered.loc[moved, "business_sk"].values,
        "from_folder_year": previous_year.loc[moved].values,
        "to_folder_year": ordered.loc[moved, "folder_year"].values,
        "from_address_sk": previous_address.loc[moved].astype("Int64").values,
        "to_address_sk": ordered.loc[moved, "address_sk"].values,
        "from_licence_number": previous_licence.loc[moved].values,
        "to_licence_number": ordered.loc[moved, "licence_number"].values,
        "changed_on": ordered.loc[moved, "year_first_issued"].values,
        "from_neighbourhood_sk": previous_area.loc[moved].astype("Int64").values,
        "to_neighbourhood_sk": ordered.loc[moved, "neighbourhood_sk"].values,
    })
    if out.empty:
        log.info("fact_address_change: no relocations found")
        return out, counts

    out["crossed_neighbourhood"] = out["from_neighbourhood_sk"].ne(out["to_neighbourhood_sk"])

    coords = dim_address.set_index("address_sk")[["latitude", "longitude"]]
    origin = coords.reindex(out["from_address_sk"]).reset_index(drop=True)
    destination = coords.reindex(out["to_address_sk"]).reset_index(drop=True)
    out["distance_km"] = _haversine_km(origin["latitude"], origin["longitude"],
                                       destination["latitude"], destination["longitude"]).round(3)

    counts["relocations"] = len(out)
    counts["crossed_neighbourhood"] = int(out["crossed_neighbourhood"].sum())

    log.info("fact_address_change: %d relocations by %d businesses, %d crossed a "
             "neighbourhood, median distance %.2f km", len(out),
             out["business_sk"].nunique(), counts["crossed_neighbourhood"],
             out["distance_km"].median(skipna=True))
    log.info("  excluded %d multi-site businesses, which is what made the first "
             "version of this wrong", counts["businesses_excluded_multi_site"])
    return out, counts
