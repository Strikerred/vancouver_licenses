"""
The three municipal questions, answered against the model.

These are here to prove the model actually supports the questions it was designed
for, and to keep the definitions in code rather than in whatever SQL somebody
writes later. Each one returns a frame and gets written as a mart.

    active_vs_closed_by_neighbourhood   question 1
    lifespan_by_category                question 2
    address_changes                     question 3

Each function documents the definition it uses, because in all three cases the
number depends more on the definition than on the arithmetic.
"""

from __future__ import annotations

import logging

import pandas as pd

from modeller.model_configs import (ACTIVITY_ACTIVE, ACTIVITY_CLOSED,
                                    ACTIVITY_LAPSED, UNKNOWN_LABEL)

log = logging.getLogger(__name__)


def active_vs_closed_by_neighbourhood(
    fact_licence: pd.DataFrame,
    dim_neighbourhood: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    """
    Active versus cancelled or expired businesses per neighbourhood, for a range.

    THE DEFINITION, which is the whole answer:

    A licence is in scope for the range if its own period overlaps the range, so
    issued on or before the end and not expired before the start. Overlap rather
    than containment, because a licence issued in December 2025 and expiring in
    December 2026 is absolutely relevant to a question about March 2026, and a
    containment test would drop it.

    Counting is at BUSINESS level, not licence level. A business with three
    amendments is one business. So this counts distinct business_sk, and only
    current revisions, otherwise every amended licence inflates its neighbourhood.

    active means the licence is Issued and had not expired as at the model's as_of
    date. cancelled and gone out of business are decisions. lapsed means expired
    with no later licence for that business, which is the one that needs the
    business key to compute and the one that a naive expired_date < today test
    gets badly wrong, because these licences are annual and nearly all of them
    expire every December.

    The Unknown neighbourhood is reported as its own row rather than dropped. Out
    of town licence holders are real and a report that silently omits them does
    not add up to the total.
    """
    scoped = fact_licence.loc[
        fact_licence["is_current_revision"]
        & (fact_licence["issued_date"].isna() | (fact_licence["issued_date"] <= end))
        & (fact_licence["expired_date"].isna() | (fact_licence["expired_date"] >= start))
    ]

    counted = (scoped.groupby(["neighbourhood_sk", "activity_state"])["business_sk"]
                     .nunique().reset_index(name="business_count"))

    wide = (counted.pivot(index="neighbourhood_sk", columns="activity_state",
                          values="business_count")
                   .fillna(0).astype("int64").reset_index())
    wide = wide.merge(dim_neighbourhood, on="neighbourhood_sk", how="left")
    wide["local_area"] = wide["local_area"].fillna(UNKNOWN_LABEL)

    for column in (ACTIVITY_ACTIVE, ACTIVITY_CLOSED, ACTIVITY_LAPSED):
        if column not in wide.columns:
            wide[column] = 0

    wide["closed_or_expired"] = wide[ACTIVITY_CLOSED] + wide[ACTIVITY_LAPSED]
    wide["total"] = wide.drop(columns=["neighbourhood_sk", "local_area",
                                       "closed_or_expired"], errors="ignore") \
                        .select_dtypes("number").sum(axis=1)
    wide["closure_rate"] = (wide["closed_or_expired"]
                            / wide["total"].where(wide["total"] > 0)).round(4)

    out = wide.sort_values(ACTIVITY_ACTIVE, ascending=False).reset_index(drop=True)
    log.info("question 1: %d neighbourhoods, %d active and %d closed or expired "
             "businesses between %s and %s", len(out), int(out[ACTIVITY_ACTIVE].sum()),
             int(out["closed_or_expired"].sum()), start.date(), end.date())
    return out


def lifespan_by_category(
    lifecycle: pd.DataFrame,
    dim_category: pd.DataFrame,
    fully_observed_only: bool = True,
) -> pd.DataFrame:
    """
    Average active lifespan in years of closed businesses, by business category.

    THE HONEST CAVEAT, which matters more than the number. Lifespan is
    first_issued_date to closure_date, and the dataset starts at folder year 2024.
    A business licensed continuously since 1998 shows a two year lifespan because
    two years is all we can see. Averaging over every closed business therefore
    measures the width of the observation window at least as much as it measures
    business survival.

    So the default restricts to businesses whose first licence is later than the
    earliest folder year in the data. Those did not exist in the first year we can
    see, so their start really is their start and their lifespan is fully
    observed. That cohort is much smaller and skewed toward short lived businesses
    by construction, since a business founded in 2025 cannot yet have lived five
    years. Both numbers are returned so the bias is visible rather than hidden:
    the restricted average is defensible, the unrestricted one is not, and the row
    counts tell you how much of the data each rests on.

    Fixing this properly needs the City's separate 1997 to 2024 historical
    datasets. That is a data availability problem, not a modelling one.
    """
    closed = lifecycle.loc[lifecycle["is_closed"] & lifecycle["lifespan_years"].notna()]
    cohort = closed.loc[closed["is_fully_observed"]] if fully_observed_only else closed

    grouped = cohort.groupby("latest_category_sk")
    out = pd.DataFrame({
        "category_sk": grouped.size().index,
        "closed_business_count": grouped.size().values,
        "avg_lifespan_years": grouped["lifespan_years"].mean().round(2).values,
        "median_lifespan_years": grouped["lifespan_years"].median().round(2).values,
        "min_lifespan_years": grouped["lifespan_years"].min().values,
        "max_lifespan_years": grouped["lifespan_years"].max().values,
    })
    out = out.merge(dim_category[["category_sk", "business_type", "business_subtype"]],
                    on="category_sk", how="left")
    out["cohort"] = "fully_observed" if fully_observed_only else "all_closed"
    out = out.sort_values("closed_business_count", ascending=False).reset_index(drop=True)

    log.info("question 2: %d categories over %d closed businesses (%s cohort), "
             "overall average %.2f years", len(out), int(out["closed_business_count"].sum()),
             out["cohort"].iloc[0] if len(out) else "n/a",
             cohort["lifespan_years"].mean() if len(cohort) else float("nan"))
    if fully_observed_only:
        log.info("  for comparison, averaging over ALL %d closed businesses gives "
                 "%.2f years, which is biased by left censoring", len(closed),
                 closed["lifespan_years"].mean() if len(closed) else float("nan"))
    return out


def address_changes(
    address_change: pd.DataFrame,
    dim_business: pd.DataFrame,
    dim_address: pd.DataFrame,
) -> pd.DataFrame:
    """
    Businesses that changed physical address between licensing periods.

    Built on fact_address_change, which only compares known addresses. A business
    moving from an unknown address to a known one is us learning where it is, not
    a relocation, so those are excluded. The consequence is that home based
    businesses, whose addresses the City has withheld since 2018, cannot appear
    here at all and this under counts by an amount the data cannot tell us.

    distance_km and crossed_neighbourhood are the columns that make this useful
    rather than trivia. Moving 200 metres and moving across the city are different
    economic events, and for a municipal question the one that matters is whether
    the business left the neighbourhood.
    """
    if address_change.empty:
        log.info("question 3: no relocations to report")
        return address_change

    labelled = address_change.merge(
        dim_business[["business_sk", "business_name", "identity_confidence"]],
        on="business_sk", how="left")

    columns = ["street", "local_area"]
    origin = dim_address[["address_sk"] + [c for c in columns if c in dim_address.columns]]
    labelled = labelled.merge(
        origin.rename(columns={"address_sk": "from_address_sk",
                               **{c: f"from_{c}" for c in columns}}),
        on="from_address_sk", how="left")
    labelled = labelled.merge(
        origin.rename(columns={"address_sk": "to_address_sk",
                               **{c: f"to_{c}" for c in columns}}),
        on="to_address_sk", how="left")

    out = labelled.sort_values("changed_on", ascending=False).reset_index(drop=True)
    log.info("question 3: %d relocations by %d businesses, %d crossed a "
             "neighbourhood boundary", len(out), out["business_sk"].nunique(),
             int(out["crossed_neighbourhood"].sum()))
    return out
