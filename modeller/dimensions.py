"""
Builds the dimension tables from the curated licence rows.

One function per dimension, each taking the curated frame and returning a frame
with a surrogate key. They are deliberately independent so you can build and
inspect one at a time.

Every dimension gets an explicit Unknown member with the reserved key. Facts
point at that instead of carrying NULL, because a NULL foreign key drops rows
from an inner join and nobody notices. A neighbourhood report that shows
'Unknown: 4,812' is honest. One that quietly totals 4,812 fewer businesses is not.
"""

from __future__ import annotations

import logging

import pandas as pd

from modeller.model_configs import (ADDRESS_COLUMNS, CLOSED_BY_ACTION,
                                    IDENTITY_CONFIDENCE, IDENTITY_LEGAL_NAME,
                                    IDENTITY_LICENCE_ONLY, IDENTITY_TRADE_NAME,
                                    STATUS_ISSUED, STATUS_PENDING, UNKNOWN_KEY,
                                    UNKNOWN_LABEL, surrogate_key)

log = logging.getLogger(__name__)


def _ascii_fold(series: pd.Series) -> pd.Series:
    """
    Strip accents before stripping punctuation, so accented letters survive as
    their base letter instead of being deleted.

    This matters more than it looks. The obvious implementation uppercases and
    then removes anything outside A-Z0-9, which turns 'Café Bleu' into 'CAF BLEU'
    and 'Crêpe & Co' into 'CRPE CO'. The accented character is not folded, it is
    thrown away, so the join key silently fails to match exactly the rows it
    exists to match. Vancouver has plenty of accented and non-Latin business
    names, so this is a real loss and not a corner case.

    NFKD splits a character into base plus combining mark, then the Mn category
    filter drops the marks and keeps the base letter.
    """
    return (series.astype("string")
                  .str.normalize("NFKD")
                  .str.encode("ascii", errors="ignore")
                  .str.decode("ascii"))


def business_identity(frame: pd.DataFrame) -> pd.DataFrame:
    """
    Assign a business_sk to every licence row, plus how we worked it out.

    Three tiers, best first: the normalised legal name, the trade name when the
    legal name is missing, and finally the licence number as a singleton for rows
    with neither. Tier three cannot be followed across years at all, so those
    businesses never show a renewal or a relocation. They are flagged rather than
    quietly mixed in.

    Identity does not use the address. Including it would make a relocation look
    like a business closing and a different one opening, which would break the
    exact question the address change table exists to answer.
    """
    out = frame.copy()

    legal = _ascii_fold(out.get("business_name")).str.upper()
    trade = _ascii_fold(out.get("business_trade_name")).str.upper()
    for series in (legal, trade):
        series.update(series.str.replace(r"[^A-Z0-9 ]", " ", regex=True)
                            .str.replace(r"\s+", " ", regex=True)
                            .str.strip())
    legal = legal.replace("", pd.NA)
    trade = trade.replace("", pd.NA)

    source = pd.Series(IDENTITY_LICENCE_ONLY, index=out.index, dtype="string")
    key = out["licence_number"].astype("string")

    use_trade = trade.notna()
    source = source.mask(use_trade, IDENTITY_TRADE_NAME)
    key = key.mask(use_trade, trade)

    use_legal = legal.notna()
    source = source.mask(use_legal, IDENTITY_LEGAL_NAME)
    key = key.mask(use_legal, legal)

    out["business_identity_key"] = key
    out["identity_source"] = source
    out["business_sk"] = pd.array([surrogate_key(k) for k in key], dtype="Int64")

    counts = source.value_counts().to_dict()
    log.info("business identity resolved from: %s", counts)
    return out


def build_dim_business(frame: pd.DataFrame) -> pd.DataFrame:
    """
    One row per business.

    The canonical name is the one on the most recent licence, on the grounds that
    if a business changed its registered name the newer one is the current truth.
    name_variant_count tells you how many distinct spellings collapsed into this
    row, which is a useful smell test: a business with eleven variants is probably
    several different businesses that share a common name.
    """
    ordered = frame.sort_values("issued_date", na_position="first")
    grouped = ordered.groupby("business_sk", dropna=False)

    dim = pd.DataFrame({
        "business_sk": grouped.size().index,
        "business_identity_key": grouped["business_identity_key"].last().values,
        "business_name": grouped["business_name"].last().values,
        "business_trade_name": grouped["business_trade_name"].last().values,
        "identity_source": grouped["identity_source"].last().values,
        "licence_count": grouped.size().values,
        "name_variant_count": grouped["business_name"].nunique(dropna=True).values,
        "first_issued_date": grouped["issued_date"].min().values,
        "last_issued_date": grouped["issued_date"].max().values,
        "first_folder_year": grouped["folder_year"].min().values,
        "last_folder_year": grouped["folder_year"].max().values,
    })
    dim["identity_confidence"] = dim["identity_source"].map(IDENTITY_CONFIDENCE)

    log.info("dim_business: %d businesses from %d licence rows", len(dim), len(frame))
    return dim


def build_dim_address(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """
    One row per distinct physical address, plus the address_sk for every licence.

    Rows with no usable address get the Unknown member. That is a lot of them:
    the City has withheld home based business addresses since 2018, so a large
    share of licences have no street at all.

    The Unknown member must never be treated as a location. Two businesses both
    sitting on UNKNOWN_KEY are not at the same address, and a move from Unknown to
    a real address is not a relocation. The address change builder skips it for
    exactly that reason.
    """
    present = [c for c in ADDRESS_COLUMNS if c in frame.columns]
    address_part = frame[present].astype("string")

    identifiable = address_part[["house", "street"]].notna().any(axis=1) \
        if {"house", "street"} <= set(present) else address_part.notna().any(axis=1)

    keys = pd.Series(UNKNOWN_KEY, index=frame.index, dtype="Int64")
    tuples = address_part.loc[identifiable].fillna("").agg("\x1f".join, axis=1)
    keys.loc[identifiable] = [surrogate_key(t) for t in tuples]

    dim = frame.loc[identifiable, present].copy()
    dim["address_sk"] = keys.loc[identifiable].values
    if {"latitude", "longitude"} <= set(frame.columns):
        dim["latitude"] = frame.loc[identifiable, "latitude"].values
        dim["longitude"] = frame.loc[identifiable, "longitude"].values
    dim = dim.drop_duplicates(subset="address_sk")
    dim["is_unknown"] = False

    unknown_row = {c: pd.NA for c in dim.columns}
    unknown_row.update({"address_sk": UNKNOWN_KEY, "is_unknown": True,
                        "local_area": UNKNOWN_LABEL})
    dim = pd.concat([pd.DataFrame([unknown_row]), dim], ignore_index=True)

    log.info("dim_address: %d distinct addresses, %d licence rows have no usable address",
             len(dim) - 1, int((~identifiable).sum()))
    return dim, keys


def build_dim_neighbourhood(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """
    One row per local planning area, plus the key for every licence.

    The City documents 22 areas. Out of town licence holders have no area at all,
    so they land on the Unknown member rather than being dropped.
    """
    area = frame["local_area"].astype("string") if "local_area" in frame.columns \
        else pd.Series(pd.NA, index=frame.index, dtype="string")

    keys = pd.Series(UNKNOWN_KEY, index=frame.index, dtype="Int64")
    known = area.notna()
    keys.loc[known] = [surrogate_key(a) for a in area.loc[known]]

    dim = pd.DataFrame({"neighbourhood_sk": keys.loc[known].values,
                        "local_area": area.loc[known].values}).drop_duplicates()
    dim = pd.concat([pd.DataFrame([{"neighbourhood_sk": UNKNOWN_KEY,
                                    "local_area": UNKNOWN_LABEL}]), dim],
                    ignore_index=True)

    log.info("dim_neighbourhood: %d areas, %d rows with no area",
             len(dim) - 1, int((~known).sum()))
    return dim, keys


def build_dim_category(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """
    One row per business type and subtype pair.

    The City streamlined over 500 categories into fewer than 100 effective
    2024-05-06. This dataset starts at folder year 2024, so licences issued in the
    first months of 2024 can still carry an old category while everything later
    uses the new taxonomy. Grouping across that boundary quietly mixes two
    different classification schemes, so the dimension flags which side of the
    change a licence's category came from and any category level analysis should
    either respect it or say it is ignoring it.
    """
    business_type = frame.get("business_type", pd.Series(pd.NA, index=frame.index))
    subtype = frame.get("business_subtype", pd.Series(pd.NA, index=frame.index))
    business_type = business_type.astype("string")
    subtype = subtype.astype("string")

    keys = pd.Series(
        [surrogate_key(t, s) if pd.notna(t) else UNKNOWN_KEY
         for t, s in zip(business_type, subtype)],
        index=frame.index, dtype="Int64")

    streamlined_from = pd.Timestamp("2024-05-06", tz="UTC")
    pre_streamline = (frame["issued_date"].notna()
                      & (frame["issued_date"] < streamlined_from))

    dim = pd.DataFrame({
        "category_sk": keys.values,
        "business_type": business_type.values,
        "business_subtype": subtype.values,
        "seen_pre_streamline": pre_streamline.values,
    })
    dim = (dim.groupby("category_sk", as_index=False)
              .agg({"business_type": "first", "business_subtype": "first",
                    "seen_pre_streamline": "any"}))
    dim.loc[dim["category_sk"] == UNKNOWN_KEY,
            ["business_type", "business_subtype"]] = UNKNOWN_LABEL
    if UNKNOWN_KEY not in set(dim["category_sk"]):
        dim = pd.concat([pd.DataFrame([{"category_sk": UNKNOWN_KEY,
                                        "business_type": UNKNOWN_LABEL,
                                        "business_subtype": UNKNOWN_LABEL,
                                        "seen_pre_streamline": False}]), dim],
                        ignore_index=True)

    log.info("dim_category: %d type/subtype pairs, %d seen before the May 2024 "
             "streamline", len(dim), int(dim["seen_pre_streamline"].sum()))
    return dim, keys


def build_dim_status(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """
    One row per status, with the flags that stop SQL hardcoding strings.

    is_closed_by_action separates Cancelled and Gone Out of Business, which are
    decisions, from expiry, which is just the calendar. That distinction is the
    whole basis of the activity rules.
    """
    status = frame["status"].astype("string")
    keys = pd.Series([surrogate_key(s) if pd.notna(s) else UNKNOWN_KEY
                      for s in status], index=frame.index, dtype="Int64")

    dim = pd.DataFrame({"status_sk": keys.values, "status": status.values}) \
        .drop_duplicates().dropna(subset=["status"])
    dim["is_closed_by_action"] = dim["status"].isin(CLOSED_BY_ACTION)
    dim["is_issued"] = dim["status"] == STATUS_ISSUED
    dim["is_pending"] = dim["status"] == STATUS_PENDING
    dim = pd.concat([pd.DataFrame([{"status_sk": UNKNOWN_KEY, "status": UNKNOWN_LABEL,
                                    "is_closed_by_action": False, "is_issued": False,
                                    "is_pending": False}]), dim], ignore_index=True)

    log.info("dim_status: %s", sorted(dim["status"].dropna().tolist()))
    return dim, keys


def build_dim_date(frame: pd.DataFrame) -> pd.DataFrame:
    """
    Day grain calendar spanning every date the data touches.

    date_sk is YYYYMMDD as an integer rather than a hash. It is the one place a
    readable key is worth more than a uniform strategy: you can read a fact row
    and know the date without a join, and range predicates work directly on it.
    """
    dates = []
    for column in ("issued_date", "expired_date", "extract_date"):
        if column in frame.columns:
            dates.append(pd.to_datetime(frame[column], errors="coerce", utc=True))
    if not dates:
        return pd.DataFrame()

    stacked = pd.concat(dates).dropna()
    if stacked.empty:
        return pd.DataFrame()

    span = pd.date_range(stacked.min().normalize(), stacked.max().normalize(),
                         freq="D", tz="UTC")
    dim = pd.DataFrame({"full_date": span})
    dim["date_sk"] = (dim["full_date"].dt.strftime("%Y%m%d")).astype("int64")
    dim["year"] = dim["full_date"].dt.year.astype("int64")
    dim["quarter"] = dim["full_date"].dt.quarter.astype("int64")
    dim["month"] = dim["full_date"].dt.month.astype("int64")
    dim["month_name"] = dim["full_date"].dt.strftime("%B")
    dim["day_of_month"] = dim["full_date"].dt.day.astype("int64")
    dim["is_month_end"] = dim["full_date"].dt.is_month_end
    dim["is_year_end"] = dim["full_date"].dt.is_year_end

    log.info("dim_date: %d days, %s to %s", len(dim),
             dim["full_date"].min().date(), dim["full_date"].max().date())
    return dim
