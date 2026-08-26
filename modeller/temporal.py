"""
Captures change over time: attribute history, status transitions, renewals.

===============================================================================
WHAT WE CAN AND CANNOT KNOW ABOUT HISTORY
===============================================================================
Worth being blunt about this first, because it bounds everything below.

The source gives us a snapshot. Each licence row has a status but no date the
status changed, so there is no event stream to read. And the raw layer overwrites
each folder year on every run rather than keeping dated snapshots, so we are not
accumulating one either. A pipeline that kept every daily extract could diff
consecutive snapshots and recover true transition dates. This one cannot.

What is recoverable is the change history the data already encodes:

    licence_revision_number runs 00 to 10, and every revision is a separate row.
    A revision is an amendment to a live licence, so a status or address that
    differs between revision 00 and 01 is an observed change with a date.

    Licences are annual, so consecutive licences for the same business are
    renewals. A status that differs between last year's licence and this year's is
    an observed change.

So transitions are derived from the licence sequence, not from snapshot diffing.
The dates are the issue dates of the records involved, which means a transition is
dated to when it was recorded rather than when it happened. For a cancellation
part way through a year that is late by up to a year, and the tables say so rather
than implying precision they do not have.

    transition_source = revision   within one licence, an amendment
    transition_source = renewal    between two licences, year over year

If snapshot retention is added later, this module is where a snapshot diff would
plug in, and the transition table's shape would not need to change.

===============================================================================
WHY TYPE 2 AND NOT A VERSION IN THE KEY
===============================================================================
dim_business_history is a type 2 dimension: one row per span during which a
business's tracked attributes held steady, with valid_from, valid_to and
is_current.

business_sk stays the identity of the business and does not encode a version. A
fact row joins to the business on business_sk alone and gets the business, or
joins on business_sk plus a date range and gets the business as it was then. If
the version were baked into the surrogate key, every fact would have to know which
version it belonged to at load time, and re-running the model with an extra day of
data could renumber versions and invalidate the facts. Keeping identity and
history in separate columns is what makes the rebuild safe.
"""

from __future__ import annotations

import logging

import pandas as pd

from modeller.model_configs import UNKNOWN_KEY

log = logging.getLogger(__name__)

FAR_FUTURE = pd.Timestamp("9999-12-31", tz="UTC")

TRACKED_ATTRIBUTES = ("business_name", "status", "category_sk",
                      "address_sk", "neighbourhood_sk")


def _ordered(frame: pd.DataFrame) -> pd.DataFrame:
    """
    Licence records in the order a business actually lived them.

    Sorted by issue date first, then licence number, then revision. Revisions of
    the same licence can share an issue date, so revision has to break the tie or
    the sequence of amendments comes out scrambled and every derived transition is
    wrong.
    """
    revision = pd.to_numeric(frame["licence_revision_number"], errors="coerce").fillna(-1)
    return (frame.assign(_revision=revision)
                 .sort_values(["business_sk", "issued_date", "licence_number", "_revision"],
                              na_position="first")
                 .drop(columns="_revision"))


def build_dim_business_history(
    frame: pd.DataFrame,
    fact_licence: pd.DataFrame,
) -> pd.DataFrame:
    """
    Type 2 history of a business's tracked attributes.

    A new row starts whenever any tracked attribute differs from the previous
    licence record for that business. valid_to is the start of the next span, so
    the ranges are half open: valid_from inclusive, valid_to exclusive. That
    convention avoids the off by one day arguments you get with inclusive end
    dates, and a between predicate on a half open range never double counts a
    business on the boundary day.

    change_reason lists which attributes moved, so you can filter the history to
    the kind of change you care about without diffing the columns yourself.
    """
    joined = fact_licence.merge(
        frame[["licence_rsn", "business_name"]], on="licence_rsn", how="left")
    ordered = _ordered(joined)

    tracked = [c for c in TRACKED_ATTRIBUTES if c in ordered.columns]
    previous = ordered.groupby("business_sk")[tracked].shift(1)

    changed_any = pd.Series(False, index=ordered.index)
    reasons = pd.Series("", index=ordered.index, dtype="string")
    for column in tracked:
        differs = ordered[column].ne(previous[column]) & previous[column].notna()
        changed_any |= differs
        reasons = reasons.mask(differs, reasons.where(reasons.eq(""),
                                                     reasons + ";").fillna("") + column)

    first_of_business = previous[tracked].isna().all(axis=1)
    span_start = changed_any | first_of_business
    ordered["_span"] = span_start.groupby(ordered["business_sk"]).cumsum()
    ordered["_reason"] = reasons.replace("", pd.NA)

    grouped = ordered.groupby(["business_sk", "_span"], dropna=False)
    history = grouped.agg(
        valid_from=("issued_date", "min"),
        business_name=("business_name", "first"),
        status=("status", "first"),
        category_sk=("category_sk", "first"),
        address_sk=("address_sk", "first"),
        neighbourhood_sk=("neighbourhood_sk", "first"),
        licence_number=("licence_number", "first"),
        change_reason=("_reason", "first"),
        record_count=("licence_rsn", "size"),
    ).reset_index()

    history = history.sort_values(["business_sk", "valid_from"], na_position="first")
    history["valid_to"] = history.groupby("business_sk")["valid_from"].shift(-1)
    history["valid_to"] = history["valid_to"].fillna(FAR_FUTURE)
    history["is_current"] = history["valid_to"].eq(FAR_FUTURE)
    history["change_reason"] = history["change_reason"].fillna("initial")
    history = history.drop(columns="_span")

    versioned = int((history.groupby("business_sk").size() > 1).sum())
    log.info("dim_business_history: %d spans over %d businesses, %d have more "
             "than one version", len(history), history["business_sk"].nunique(), versioned)
    return history


def build_fact_status_transition(fact_licence: pd.DataFrame) -> pd.DataFrame:
    """
    One row per observed status change.

    Consecutive licence records for the same business where the status differs.
    transition_source says whether it happened inside one licence, meaning an
    amendment, or between two licences, meaning it was noticed at renewal.

    days_in_previous_status is the gap between the two records, not the true
    duration of the status. We only know when a status was recorded, so this is an
    upper bound on how long the previous status had been true. Named for what it
    measures rather than what somebody might wish it measured.
    """
    ordered = _ordered(fact_licence)

    previous_status = ordered.groupby("business_sk")["status"].shift(1)
    previous_date = ordered.groupby("business_sk")["issued_date"].shift(1)
    previous_licence = ordered.groupby("business_sk")["licence_number"].shift(1)

    changed = (previous_status.notna() & ordered["status"].notna()
               & previous_status.ne(ordered["status"]))

    out = pd.DataFrame({
        "business_sk": ordered.loc[changed, "business_sk"].values,
        "from_status": previous_status.loc[changed].values,
        "to_status": ordered.loc[changed, "status"].values,
        "effective_date": ordered.loc[changed, "issued_date"].values,
        "previous_date": previous_date.loc[changed].values,
        "from_licence_number": previous_licence.loc[changed].values,
        "to_licence_number": ordered.loc[changed, "licence_number"].values,
    })
    if out.empty:
        log.info("fact_status_transition: none found")
        return out

    out["transition_source"] = out["from_licence_number"].eq(out["to_licence_number"]) \
        .map({True: "revision", False: "renewal"})
    out["days_in_previous_status"] = (out["effective_date"] - out["previous_date"]).dt.days

    log.info("fact_status_transition: %d transitions (%s)", len(out),
             out["transition_source"].value_counts().to_dict())
    log.info("  most common: %s",
             (out["from_status"] + " -> " + out["to_status"]).value_counts().head(5).to_dict())
    return out


def build_fact_renewal(fact_licence: pd.DataFrame) -> pd.DataFrame:
    """
    One row per renewal: a business holding a licence in one year and the next.

    AT YEAR GRAIN, not licence to licence, for the same reason the address change
    table is. A business with three simultaneous premises holds three licences a
    year, and walking consecutive licence records makes each one look like a
    renewal of the last. That counted 124,776 renewals against 74,716 businesses,
    which is really 'this business has several branches' dressed up as churn.

    So each business gets collapsed to one row per folder year, and a renewal is
    consecutive years. gap_days is the first issue date of the new year minus the
    last expiry of the previous year.

    Negative or zero means renewed before the old licence lapsed, the normal case.
    Positive means the business was unlicensed for that many days and came back,
    which is a genuinely different event: a temporary lapse, not a closure.
    Separating those two is most of the value here, because a naive read of the
    licence data counts the gap as a business dying and a new one being born.

    year_gap greater than 1 means the business skipped one or more years entirely
    and returned, which is worth its own flag rather than being lumped in with a
    two week gap over new year.
    """
    current = fact_licence.loc[fact_licence["is_current_revision"]]

    per_year = current.groupby(["business_sk", "folder_year"], as_index=False).agg(
        licence_count=("licence_number", "nunique"),
        first_issued=("issued_date", "min"),
        last_expired=("expired_date", "max"),
        status_at_year=("status", "last"),
    )

    ordered = per_year.sort_values(["business_sk", "folder_year"])
    previous_year = ordered.groupby("business_sk")["folder_year"].shift(1)
    previous_expiry = ordered.groupby("business_sk")["last_expired"].shift(1)
    previous_issued = ordered.groupby("business_sk")["first_issued"].shift(1)

    is_renewal = previous_year.notna()

    out = pd.DataFrame({
        "business_sk": ordered.loc[is_renewal, "business_sk"].values,
        "from_folder_year": previous_year.loc[is_renewal].values,
        "to_folder_year": ordered.loc[is_renewal, "folder_year"].values,
        "previous_first_issued": previous_issued.loc[is_renewal].values,
        "previous_last_expired": previous_expiry.loc[is_renewal].values,
        "renewed_on": ordered.loc[is_renewal, "first_issued"].values,
        "licences_held": ordered.loc[is_renewal, "licence_count"].values,
        "status_after_renewal": ordered.loc[is_renewal, "status_at_year"].values,
    })
    if out.empty:
        log.info("fact_renewal: none found")
        return out

    out["gap_days"] = (out["renewed_on"] - out["previous_last_expired"]).dt.days
    out["year_gap"] = (out["to_folder_year"] - out["from_folder_year"]).astype("Int64")
    out["renewed_before_expiry"] = out["gap_days"].le(0)
    out["lapsed_then_renewed"] = out["gap_days"].gt(0)
    out["skipped_a_year"] = out["year_gap"].gt(1)

    log.info("fact_renewal: %d renewals by %d businesses, %d before expiry, "
             "%d after a lapse, %d skipped a year", len(out),
             out["business_sk"].nunique(),
             int(out["renewed_before_expiry"].sum(skipna=True)),
             int(out["lapsed_then_renewed"].sum(skipna=True)),
             int(out["skipped_a_year"].sum(skipna=True)))
    return out
