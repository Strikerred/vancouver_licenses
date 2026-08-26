"""
Schema and data quality rules for Vancouver business licences.

This is the contract for the curated layer. Everything the transformer does is
driven from here, so changing a type or a check is a config change and not a code
change, same idea as the downloader configs.

Naming
------
Raw column names are all squashed together (licencersn, folderyear). Curated
names are snake_case (licence_rsn, folder_year). I rename on the way through
because the curated layer is what people query, and it should not inherit the
portal's spelling habits.

Why licence_rsn is the primary key
----------------------------------
The publisher says licencersn is the unique identifier and that licence numbers
get reused. I checked: it is unique across all 69,889 rows of 2025, and every
value in the data is digits only, so it casts to int64 cleanly.

It is NOT unique in the live 2026 partition, where 4 rows break it. Those are two
versions of the same licence caught mid update. The downloader writes them as
received on purpose, because picking a winner needs a tie break rule, and this is
where that rule lives. See DEDUPE_ORDER below.

(licencenumber, licencerevisionnumber) is not unique either, 11 collisions in
2025 and 6 in 2026, so it is not a fallback key.

Severity model
--------------
Two levels, because "invalid" covers two very different situations.

    REJECT  the row cannot be trusted as a record at all, so it goes to
            quarantine and stays out of the curated table. Null or non numeric
            primary key, or losing a primary key tie break.

    FLAG    the row is usable but something about it is worth knowing. It stays
            in the curated table with a dq_flags column listing what tripped.

I split it that way because dropping a row is destructive and it hides the
problem. A future dated licence is a real licence, and 18 of them exist in 2026,
all status Issued and dated the 1st of a coming month. Those look like scheduled
issuances, not corruption. If I dropped them the count would silently disagree
with the source and somebody would spend an afternoon working out why.

The tie break order
-------------------
DEDUPE_ORDER is most complete row first, then newest extract, then highest
revision. Fewest nulls wins because that keeps the version which actually has the
address. The last key is only there to make the sort deterministic: two runs over
the same input have to produce the same output, and a sort that leaves ties
unresolved does not guarantee that.

Why the dtypes have capital letters
-----------------------------------
Int64 and boolean, not int64 and bool. The capitalised ones are pandas' nullable
dtypes and they can hold NA. A plain int64 column cannot, so a missing employee
count would land as 0 and be indistinguishable from a real zero. Which matters
more than usual here, see below.

Casing policy
-------------
Deliberately inconsistent. TITLE_CASE for geographic and address text so that
'renfrew-collingwood' and 'Renfrew-Collingwood' group together. UPPER_CASE for
province and country, which are two character codes.

PRESERVE_CASE for business names, and that is the one people argue about. Title
casing 'MILANO GLOBAL DEVELOPMENT CORP.' gives you something tidier and wrong: it
is not the registered name, and the same rule turns 'ABC Holdings ULC' into 'Abc
Holdings Ulc'. A name is identity, so it keeps whatever the registry has. For
matching there is a derived business_name_key, uppercased with punctuation
stripped, so you get a join key without corrupting the real value.

Fields to be careful with
-------------------------
number_of_employees: the publisher documents 0 as none and 000 as unknown. Both
arrive as 0.0, so they are indistinguishable in this field. You cannot average
this column and get anything meaningful. AMBIGUOUS_ZERO_COLUMNS records that.

status: the published field description abbreviates one value as 'GOB'. The data
says 'Gone Out of Business'. VALID_STATUSES follows the data.

postal_code: validated against the Canadian pattern rather than assumed. In this
dataset it already arrives as 'A1A 1A1', but out of town addresses can carry
anything, and a US zip is real information rather than something to null out.

Coordinate bounds: the global lat/lon range is a hard check, because outside it
the value is not a coordinate at all. BC_BOUNDING_BOX is only a soft signal, since
out of town licence holders are legitimate and the City simply does not map them.
"""

from __future__ import annotations


PRIMARY_KEY = "licence_rsn"

DEDUPE_ORDER = [
    ("_null_count", True),
    ("extract_date", False),
    ("licence_revision_number", False),
]


COLUMNS: dict[str, tuple[str, str]] = {
    "licencersn":            ("licence_rsn", "Int64"),
    "licencenumber":         ("licence_number", "string"),
    "licencerevisionnumber": ("licence_revision_number", "Int64"),
    "folderyear":            ("folder_year", "Int64"),
    "businessname":          ("business_name", "string"),
    "businesstradename":     ("business_trade_name", "string"),
    "status":                ("status", "string"),
    "issueddate":            ("issued_date", "timestamp"),
    "expireddate":           ("expired_date", "date"),
    "extractdate":           ("extract_date", "timestamp"),
    "businesstype":          ("business_type", "string"),
    "businesssubtype":       ("business_subtype", "string"),
    "unit":                  ("unit", "string"),
    "unittype":              ("unit_type", "string"),
    "house":                 ("house", "string"),
    "street":                ("street", "string"),
    "city":                  ("city", "string"),
    "province":              ("province", "string"),
    "country":               ("country", "string"),
    "postalcode":            ("postal_code", "string"),
    "localarea":             ("local_area", "string"),
    "numberofemployees":     ("number_of_employees", "Int64"),
    "feepaid":               ("fee_paid", "float64"),
}

TWO_DIGIT_YEAR_COLUMNS = ("folder_year",)

GEO_SOURCE_COLUMNS = ("geom", "geo_point_2d")


TITLE_CASE_COLUMNS = ("city", "street", "unit_type", "local_area")
UPPER_CASE_COLUMNS = ("province", "country")
PRESERVE_CASE_COLUMNS = ("business_name", "business_trade_name", "licence_number",
                         "status", "business_type", "business_subtype")

NAME_KEY_SOURCE = "business_name"
NAME_KEY_COLUMN = "business_name_key"

POSTAL_CODE_COLUMN = "postal_code"
POSTAL_CODE_PATTERN = r"^[ABCEGHJ-NPRSTVXY]\d[ABCEGHJ-NPRSTV-Z] \d[ABCEGHJ-NPRSTV-Z]\d$"


REQUIRED_NOT_NULL = ("licence_rsn", "licence_number", "status", "folder_year")

VALID_STATUSES = ("Issued", "Pending", "Cancelled", "Inactive", "Gone Out of Business")

LATITUDE_BOUNDS = (-90.0, 90.0)
LONGITUDE_BOUNDS = (-180.0, 180.0)
BC_BOUNDING_BOX = {"lat": (48.0, 60.0), "lon": (-139.5, -114.0)}

AMBIGUOUS_ZERO_COLUMNS = ("number_of_employees",)
