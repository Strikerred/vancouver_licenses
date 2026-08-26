"""
Model configuration: surrogate keys, identity rules, status classification.

Everything the modeller decides is driven from here so the definitions live in one
place. The definitions are the model. The schema is the easy part.

===============================================================================
SURROGATE KEY STRATEGY
===============================================================================
All surrogate keys are deterministic hashes of the natural key, not sequential
integers.

    business_sk = blake2b(business_name_key)  -> signed int64
    address_sk  = blake2b(normalised address tuple)
    date_sk     = YYYYMMDD as an integer
    category_sk = blake2b(business_type + business_subtype)

Why hashes and not a sequence. This pipeline has no state: there is no key table
and no database sequence to draw from, and parquet cannot generate one. If I
assigned keys by row order, a rebuild would hand out different keys for the same
businesses and every saved query, extract and dashboard downstream would silently
join to the wrong rows. A hash of the natural key gives the same answer on every
run, on every machine, in any order.

WHY NOT python's hash(). It is salted per process, so hash('abc') differs between
runs. Using it for a surrogate key is a bug that only shows up after a restart.
blake2b is stable.

The trade offs I am accepting:

    Wider keys. int64 rather than a small int. Irrelevant at 200k rows, would
    matter at a billion.

    Not human readable. You cannot eyeball business_sk 4823... and know who it
    is. Mitigated by keeping business_name_key on the dimension.

    Collision risk. Two different names hashing to the same int64. At 64 bits and
    ~130k businesses the probability is about 1 in 10^9. The build asserts key
    uniqueness anyway, so a collision fails the run rather than corrupting a
    join.

    THEY MUST STAY IN A NULLABLE INTEGER DTYPE. This one cost me an afternoon.
    A 64 bit hash does not survive a trip through float64: the mantissa is 53
    bits, so the value gets rounded. pandas promotes a plain int64 column to
    float64 the moment an operation needs to hold a missing value, and
    groupby().shift(1) does exactly that. The shifted key comes back rounded,
    casting it back to int64 gives a number that was never a real key, and every
    join against it silently returns nothing.

    It showed up as 1,283 of 1,288 relocations having a from_address that did not
    exist in dim_address, which read as null distances and a 99.9% neighbourhood
    crossing rate. Plausible looking, completely fabricated. So every surrogate
    key column is Int64, the nullable one, which carries pd.NA instead of NaN and
    never leaves integer space.

    No slowly-changing-dimension versioning in the key itself. business_sk
    identifies the business, not a version of it. Attribute history lives in
    dim_business_history with valid_from and valid_to, which is a type 2 pattern
    without putting a version number in the key.

UNKNOWN MEMBERS. Every dimension has a reserved key for unknown, and facts point
at it instead of carrying NULL. A NULL foreign key silently drops rows from an
inner join, which is exactly the kind of quiet wrongness this whole project is
trying to avoid. So an address we cannot identify gets UNKNOWN_KEY, not NULL, and
a count of businesses per neighbourhood shows the unknowns as their own row rather
than losing them.

===============================================================================
BUSINESS IDENTITY, AND WHY IT IS THE HARD PART
===============================================================================
This dataset has no business identifier. licence_rsn identifies one licence
record. licence_number is YY-NNNNNN, so a renewal next year gets an entirely new
number. Nothing links a business to its own past except the name and the address.

Two of the three questions need to follow a business through time, so identity has
to be constructed. The rule, in order:

    1. business_name_key   normalised, ASCII folded business name
    2. business_trade_name key, when the legal name is missing (6 to 7% of rows)
    3. the licence number itself, as a singleton

Identity deliberately does NOT include the address. If it did, a business moving
premises would look like one business dying and another being born, which is the
exact opposite of what the address change question is asking for.

What this costs, stated plainly:

    Two unrelated businesses with the same name merge into one. There is no way
    to separate them from this data alone. dim_business carries
    identity_confidence so you can exclude the risky ones.

    A business that changes its registered name splits into two. Common with
    incorporations, 'Joe Smith' becoming 'Joe Smith Holdings Ltd'.

    Rows in tier 3 cannot be tracked across years at all, so they never show a
    renewal or a relocation. They are flagged, not silently included.

A better version of this needs an external registry to match against, BC
corporate registration numbers or similar. Worth saying out loud rather than
implying the name match is sound.

===============================================================================
STATUS AND ACTIVITY
===============================================================================
The published status has five values. Activity is NOT the same thing as status,
and conflating them is the easiest way to get the neighbourhood question wrong.

These licences are annual. Almost all of them expire on 31 December and are
renewed. So expired does not mean closed: a naive `expired_date < today` counts
every healthy renewed business as dead. A licence that has expired only tells you
the business closed if no later licence exists for the same business.

    active            status Issued and the licence period covers the date
    superseded        expired, but a later licence exists, so it was renewed
    lapsed            expired, and NO later licence for this business
    closed_by_action  status Cancelled or Gone Out of Business
    inactive          status Inactive
    pending           status Pending, never issued

superseded is the state I originally forgot, and forgetting it is expensive.
Without it every expired-but-renewed licence falls through the classification and
lands in whatever the default is, which made 54% of the dataset look closed. An
expired licence belonging to a business that renewed is not a closure, it is last
year's paperwork.

lapsed is the one that needs the constructed business key to compute, and it is
where I would expect most implementations to be wrong.
"""

from __future__ import annotations

import hashlib

UNKNOWN_KEY = -1
UNKNOWN_LABEL = "Unknown"

STATUS_ISSUED = "Issued"
STATUS_PENDING = "Pending"
STATUS_CANCELLED = "Cancelled"
STATUS_INACTIVE = "Inactive"
STATUS_GONE = "Gone Out of Business"

CLOSED_BY_ACTION = (STATUS_CANCELLED, STATUS_GONE)

ACTIVITY_ACTIVE = "active"
ACTIVITY_SUPERSEDED = "superseded"
ACTIVITY_CLOSED = "closed_by_action"
ACTIVITY_LAPSED = "lapsed"
ACTIVITY_INACTIVE = "inactive"
ACTIVITY_PENDING = "pending"
ACTIVITY_UNKNOWN = "unknown"

IDENTITY_LEGAL_NAME = "legal_name"
IDENTITY_TRADE_NAME = "trade_name"
IDENTITY_LICENCE_ONLY = "licence_only"

IDENTITY_CONFIDENCE = {
    IDENTITY_LEGAL_NAME: "medium",
    IDENTITY_TRADE_NAME: "low",
    IDENTITY_LICENCE_ONLY: "none",
}

ADDRESS_COLUMNS = ("house", "street", "unit", "unit_type", "postal_code",
                   "city", "province", "local_area")

EARTH_RADIUS_KM = 6371.0

MODEL_TABLES = (
    "dim_business",
    "dim_address",
    "dim_neighbourhood",
    "dim_category",
    "dim_status",
    "dim_date",
    "dim_business_history",
    "fact_licence",
    "fact_business_lifecycle",
    "fact_status_transition",
    "fact_renewal",
    "fact_address_change",
)


def surrogate_key(*parts: object) -> int:
    """
    Deterministic signed int64 from the natural key parts.

    blake2b rather than python's hash() because hash() is salted per process, so
    it gives a different answer after a restart. A surrogate key that changes
    between runs breaks every downstream join, and it does it quietly.

    None and empty parts are normalised so that ('a', None) and ('a', '') produce
    the same key. Otherwise the same address written two slightly different ways
    lands in the dimension twice.
    """
    normalised = "\x1f".join("" if p is None else str(p).strip() for p in parts)
    digest = hashlib.blake2b(normalised.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big", signed=True)
