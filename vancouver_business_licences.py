"""
City of Vancouver business licences: download, transform, model.

Single file version of a three stage pipeline. Everything is in here, including
the documentation, so it can be pasted into one box and run.

    pip install pandas requests pyarrow

    python vancouver_business_licences.py                 # all three stages
    python vancouver_business_licences.py --stage download
    python vancouver_business_licences.py --stage transform
    python vancouver_business_licences.py --stage model
    python vancouver_business_licences.py --since 2026-08-17 --until 2026-08-23
    python vancouver_business_licences.py --partition 2026
    python vancouver_business_licences.py --start 2026-01-01 --end 2026-12-31
    python vancouver_business_licences.py --dry-run

Exit codes: 0 clean, 1 something failed, 2 bad config or a year that is not
upstream. Output goes under the working directory.

===============================================================================
THE THREE STAGES
===============================================================================
STAGE 1, download. Business licence records from 2024 onwards into raw JSON, one
file per folder year. Daily incremental: it probes each partition cheaply and
only re-downloads the ones that moved upstream.

    data/vancouver/business-licences/<year>/records.json

STAGE 2, transform. Casts the raw strings into a typed schema, standardises text
and postal codes, pulls latitude and longitude out of the nested geojson, runs
the data quality checks, enforces the primary key, writes parquet one file per
issued date.

    curated/vancouver/business-licences/<year>/<month>/<date>.parquet

STAGE 3, model. A star schema over the curated layer, plus three marts answering
active versus closed businesses per neighbourhood, average lifespan of closed
businesses by category, and businesses that changed address.

    model/<dim|fact>_*.parquet
    model/marts/mart_*.parquet

===============================================================================
SECTIONS IN THIS FILE
===============================================================================
    CONFIGURATION      what to pull, the curated schema, the model definitions
    TASK 1             portal client, downloader, download orchestration
    TASK 2             reader, combiner, manipulators, data quality, exporter
    TASK 3             dimensions, facts, temporal, the three questions
    COMMAND LINE       the CLI

Each section keeps its own docstring explaining that piece. The reasoning for the
choices is in those docstrings rather than in inline comments.

===============================================================================
STAGE 1 - RAW
===============================================================================


data/vancouver/business-licences/2024|2025|2026/records.json


205,105 rows, which matches the publisher's own records_count exactly. The
pipeline reconciles those two numbers every run and puts it in the summary line,
because there is no manifest file so the log is the audit trail.

Records land exactly as they came back. No renaming, no retyping, no dropping
columns. I deliberately do not put the raw output through pandas, because a
DataFrame round trip turns None into NaN (not valid JSON), promotes ints to
floats, and mangles the nested geom objects. If raw has already been retyped you
cannot use it to check the transform, which is the whole point of keeping it.

What I found in the API before writing anything
-----------------------------------------------

I checked all of this against the live API. Where the docs and the data
disagreed, I went with the data.

| What I found | What I did about it |
|---|---|
| /records caps out at limit<=100 and offset+limit<=10000, and the dataset has 205,105 rows | You can only ever see the first 10,000, and it returns 200 the whole way. So offset paging gives you 5% of the data and looks fine. Used /exports/json?limit=-1, which is also what their own swagger tells you to do |
| 28,720 rows (14%) have issueddate NULL, all Pending plus some Cancelled and Inactive | Filtering "issued from 2024 onwards" on issueddate silently drops all of them. Partitioned on folderyear instead, it is filled in on every row |
| extractdate is the same for every row in a folder year | It is a batch stamp, not a per row change token. No row level delta exists in this API, so the incremental works per partition |
| Only the current folder year refreshes daily | Matches their "Daily (for current year extracts)" note. A normal day is one download and a few cheap probes |
| licencersn is unique in 2025 but not in 2026, where 4 rows break it | Warn and land it as received. Picking a winner is a transform decision, see below |
| numberofemployees is a double, and their doc says 0 = none, 000 = unknown | Both end up as 0.0, so real zero and unknown are the same value. You cannot average this column |
| status in the data is Gone Out of Business, the field description calls it GOB | Code against the data, not the description |

The incremental
---------------


for each partition (discovered, not hardcoded):
    probe                -> count(*) and max(extractdate), one row, cheap
    compare against disk -> row count and max extractdate of the file we have
    moved? -> download, validate, write.   same? -> skip


There is no state file. The data on disk is the state. I read the row count
and the max extractdate out of the file we wrote last time. Better than a
sidecar because the two can never disagree, and with a state file you eventually
end up debugging your own bookkeeping instead of the data. Costs a second or two
of parsing per partition.

| Case | Result |
|---|---|
| Cold start | 3 written, 205,105 rows, reconciles |
| Nothing changed | 3 skipped in 3.1s, zero downloads |
| One year deleted | Only that year re-pulled |
| One year corrupted | Detected on parse, warned, re-pulled |

What it cannot do: intra day history. If a status changes twice between two
extracts we only see the final one. That is the source, not the pipeline.

===============================================================================
STAGE 2 - CURATED
===============================================================================


curated/vancouver/business-licences/<year>/<month>/<YYYY-MM-DD>.parquet
curated/vancouver/business-licences/<year>/<month>/<YYYY-MM-DD>.quarantine.parquet
curated/vancouver/business-licences/<year>/unknown/no-issued-date.parquet


1,020 daily files, 205,101 rows, licence_rsn unique. That is 205,105 raw minus
4 rejected duplicates.

Year and month come from issued_date, because that is the business date people
filter on. One file per day means a daily run writes one file and re-running a day
replaces exactly that file, unlike the raw layer which rewrites a whole year.

Types, verified on disk with pyarrow
------------------------------------

| Column | Parquet type |
|---|---|
| licence_rsn (primary key) | int64 |
| issued_date, extract_date | timestamp[us, tz=UTC] |
| expired_date | date32[day] |
| latitude, longitude | double |
| fee_paid | double |
| number_of_employees, folder_year, licence_revision_number | int64 |
| postal_code_valid, has_coordinates | bool |

Raw names are squashed together (licencersn), curated names are snake_case
(licence_rsn). The curated layer is what people query and it should not inherit
the portal's spelling habits.

Anything that will not cast becomes NA and the count is logged. Never a guess,
never a silent zero. The nullable dtypes (Int64, not int64) matter here: a
plain int64 cannot hold NA, so a missing employee count would land as 0 and be
indistinguishable from a real zero.

Casing
------

Deliberately inconsistent, and worth explaining. city, street, local_area
and unit_type get title cased so they group. province and country are two
character codes so they go upper. Business names are left exactly as they
arrive. Title casing MILANO GLOBAL DEVELOPMENT CORP. gives you something
tidier and wrong, and the same rule turns ABC Holdings ULC into
Abc Holdings Ulc. A name is identity. For matching there is a derived
business_name_key, uppercased with punctuation stripped, so you get a join key
without corrupting the real value.

Geo
---

geo_point_2d is the primary source and geom the fallback. I checked and on the
rows where both exist they match exactly.

GeoJSON coordinates are [longitude, latitude], in that order. Reading them
the wrong way round puts Vancouver in the Indian Ocean and nothing errors, you
just get a map with no pins. Same family as reading a UTC price against a local
time volume. The index is spelled out in the code and the bounds check exists to
catch it if someone edits it later. Coordinate strings are handled too, because
the CSV export of this dataset writes lat, lon while the GeoJSON writes
[lon, lat].

103,072 rows have coordinates. Having none is normal, not a problem: home based
addresses are withheld and out of town addresses were never geocoded.

Primary key and the tie break
-----------------------------

licence_rsn, enforced not assumed. The transform refuses to write if the key is
still not unique afterwards.

The 4 duplicates are not duplicate rows, they are two versions of the same licence
caught mid update. The tie break is most complete row first (fewest nulls), then
newest extract, then highest revision. The last key is only there so two runs over
the same input always give the same answer.

It picks correctly. For 4940392 it keeps Bard on the Beach Theatre Society
with a full address and quarantines Bard on the Beach with every address field
null.

Data quality
------------

Two severities, because "invalid" covers two different situations.

Reject - not a usable record, goes to *.quarantine.parquet and stays out of
the curated table: null or non numeric primary key, or losing a key tie break.

Flag - usable but worth knowing, stays in the table with the reason in
dq_flags:

| Flag | Rows |
|---|---|
| postal_code_not_canadian | 150 |
| expired_before_issued | 94 |
| future_issued_date | 18 |

Flagging rather than dropping is deliberate. The 18 future dated rows are all
status Issued and all dated the 1st of a coming month, so they are almost
certainly scheduled issuances rather than corruption. Drop them and the curated
count silently disagrees with the source.

Quarantine sits next to the day it came from rather than in its own tree, so
anyone looking at a day's output trips over the rejects instead of having to know
they exist. It is also not windowed: a rejected row is a fact about the data,
not about the week we happen to be writing.

The 28,717 rows with no issued_date go to <year>/unknown/. They are Pending
licences and they are real records, so they get a named folder rather than being
quietly dropped.

===============================================================================
HANDLING STRATEGY
===============================================================================

Every condition I check for, what happens to the row, and why. The two things I
refuse to do are drop a row silently and let a bad row through unmarked.

| Condition | Severity | What happens to the row | Where it ends up |
|---|---|---|---|
| /records paging cap | n/a | avoided by design, /exports/json instead | - |
| Empty extract from the API | abort | nothing is written, run exits non zero | raw stays as it was |
| Partition shrank >10% vs what we hold | abort | refuse to overwrite, exit non zero | raw stays as it was |
| Unknown column appeared upstream | abort | refuse to ingest until the contract is reviewed | raw stays as it was |
| Rows carrying the wrong folderyear | abort | the upstream filter did not hold, so nothing is trusted | raw stays as it was |
| Raw file missing or corrupt locally | recover | treat the partition as new and re-download | raw rewritten |
| Value will not cast to its type | flag | set to NA, count logged per column | curated, NA in that column |
| licence_rsn null or non numeric | reject | removed from curated | *.quarantine.parquet |
| licence_rsn duplicated | reject the loser | tie break keeps one, the other is removed | *.quarantine.parquet |
| licence_rsn still duplicated after the tie break | abort | raise, refuse to write a table claiming a key it does not have | nothing written |
| Future dated issued_date | flag | kept | curated, dq_flags=future_issued_date |
| expired_date before issued_date | flag | kept | curated, dq_flags=expired_before_issued |
| Status not one of the five documented | flag | kept | curated, dq_flags=unknown_status |
| Postal code not Canadian format | flag | cleaned value kept, not nulled | curated, postal_code_valid=false |
| Coordinates outside valid lat/lon | flag | kept | curated, dq_flags=coordinates_out_of_bounds |
| No coordinates at all | ignore | kept, has_coordinates=false | curated, not flagged |
| Null in a required non-key column | flag | kept | curated, dq_flags=null_<column> |
| No issued_date (28,717 Pending rows) | route | kept, cannot be placed on a calendar | <year>/unknown/ |

Three principles behind that:

Reject means quarantined, never deleted. A rejected row goes to parquet next
to the day it came from, with the reason in dq_reject_reason. A log line gets
rotated away; a parquet file can be queried in a month when somebody asks what
happened to a licence.

Flag rather than drop wherever the row is still a real record. Dropping is
destructive and it hides the problem, and the counts stop matching the source with
no explanation.

Abort rather than write something wrong. If the extract itself cannot be
trusted, no output at all is better than plausible bad output, because plausible
bad output gets used.

And one thing I deliberately do not do: alert on conditions that are normal here.
Missing addresses, missing coordinates and 22% null country are all structural,
so they are profiled and not warned about. Null rates only warn when they *move*
more than 5%. A pipeline that shouts every day about something normal is a
pipeline whose warnings nobody reads.

===============================================================================
ASSUMPTIONS
===============================================================================

Things I could not verify from the API and had to decide. Each one is a place this
would need revisiting if it turns out to be wrong.

folderyear is the year of issue. The publisher documents it as the first two
characters of the licence number, representing the year issued, and I use it as
the partition key for "2024 onwards". If it is really an administrative year
rather than an issue year then the raw scope is subtly wrong. Some evidence it is
at least not the calendar year of issued_date: folder year 2024 contains rows
issued as early as 2023-11-04.

Timestamps really are UTC. issueddate and extractdate arrive with a
+00:00 offset and I parse them as UTC. If the publisher is actually writing
Vancouver local time and labelling it +00:00, every timestamp in the curated
layer is 7 or 8 hours out and nothing would reveal it. I cannot check this from
the API alone. It is the assumption I would want confirmed first, because it is
both invisible and pervasive - the same shape as the timezone defect I have hit in
production before.

licence_rsn is meant to be unique and the collisions are transient. It is
unique across all 69,889 rows of the closed 2025 partition and broken by 4 rows in
the live 2026 one, so I treat those as records caught mid update rather than as
the key being unreliable. If collisions ever appear in a closed year, the key
choice needs rethinking.

The more complete row wins a tie. When a key collides I keep the row with
fewest nulls. That is a judgment call, not something the City tells us. It gives
the right answer on all 4 cases here, but if the City's real rule is "latest
revision wins regardless" then some of these are backwards.

extractdate moving means the partition was republished. The whole incremental
rests on it. I hedge by also comparing row counts, so a republish that changed
content without moving the stamp still gets caught, but a republish that changed
neither would be invisible.

Future dated issuances are scheduled, not errors. All 18 are status Issued
and dated the 1st of a coming month, which reads like advance issuance. So they are
flagged and kept. If they are actually data entry errors they should be rejected,
and that is a one line config change.

The 94 expired_before_issued rows are legitimate or unknown, not corrupt, so
they are flagged rather than rejected. Backdated renewals would explain them. This
is the one I would most want a business answer on.

Business names should not be recased. I keep the registry's casing and derive
a separate business_name_key for joins. If the consumer would rather have pretty
display names, that is a different choice and it belongs in a view, not here.

A daily schedule is enough. The publisher refreshes the current folder year
daily and I have not seen intra day updates, so a single daily run should not miss
anything. It does mean intra day status changes are invisible.

The dataset only holds 2024 onwards. The publisher says the categories were
streamlined in May 2024 and the history lives in separate datasets, and the only
folder years present are 24, 25 and 26. The year filter guards against older rows
appearing anyway, but those historical datasets are not ingested here.

===============================================================================
SCHEDULE
===============================================================================

Daily. Their current year extract landed around 07:08 UTC the day I built this,
so about 12:00 UTC leaves room for a late publish and we still get same day data.

Stage 2 transforms every year whenever stage 1 downloaded anything, for the
overlap reason above. About fifteen seconds for 205k rows.

===============================================================================
STAGE 3, THE DIMENSIONAL MODEL
===============================================================================
A star schema: fact_licence at the licence record grain in the middle, the
dimensions around it, and derived tables that pre-compute the sequence work so a
question does not need a window function over 200k rows every time.

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

Surrogate keys
--------------
Deterministic blake2b hashes of the natural key, not sequential integers, because
this pipeline has no state to draw a sequence from and a rebuild has to produce
the same keys or every saved query downstream joins to the wrong rows. Not
python's hash(), which is salted per process and changes after a restart.

They are all nullable Int64. A 64 bit hash does not survive float64, which has a
53 bit mantissa, and pandas promotes int64 to float64 the moment something needs
to hold a missing value. groupby().shift() does exactly that.

Every dimension has a reserved Unknown member and facts point at it instead of
carrying NULL, because a NULL foreign key silently drops rows from an inner join.

There is no business identifier in this data
--------------------------------------------
licence_rsn identifies a licence record and licence_number is YY-NNNNNN, so a
renewal gets a brand new number. Identity has to be constructed: normalised legal
name, falling back to trade name, falling back to the licence number as a
singleton. It deliberately does not include the address, or a relocation would
look like one business closing and another opening.

What that costs: two unrelated businesses sharing a name merge, a business that
changes its registered name splits in two, and singletons cannot be tracked across
years at all. dim_business carries identity_confidence so the risky ones can be
excluded. Doing this properly needs an external registry to match against.

Expiry is not closure
---------------------
These licences are annual and nearly all of them expire on 31 December and get
renewed. expired_date < today counts every healthy business as dead. A licence
that expired only means closure if no later licence exists for the same business,
which is a lookahead and not a row level test. Hence the superseded state.

Lifespan is left censored
-------------------------
The data starts at folder year 2024, so a business licensed since 1998 shows a two
year lifespan. Averaging over all closed businesses measures the observation
window as much as it measures survival. The default restricts to the cohort whose
first licence is after the earliest year held, and both numbers are logged. Note
78% of that cohort is Short-term Rental Operator, so the headline number is really
measuring short term rental churn.

Capturing change over time
--------------------------
There are no status change events in the source, and the raw layer overwrites each
folder year rather than keeping dated snapshots, so there is no history to diff.
What is recoverable is what the data already encodes: licence_revision_number runs
00 to 10 and every revision is an amendment, and consecutive folder years are
renewals. Transitions are derived from that sequence, which means they are dated to
when the change was recorded rather than when it happened.

"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import tempfile
import time
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
import requests

log = logging.getLogger("licences")

PROJECT_ROOT = Path.cwd()
# ===============================================================================
# CONFIGURATION - WHAT TO PULL
# ===============================================================================

"""
Config for the open data pipeline.

Adding a new city should be a config entry, not a code change. That is the whole
reason this file exists. I have done the other version before, where every new
source means new code, and it does not scale.

How it is organized
-------------------
    SHARED_CONFIG               defaults for everything
    PORTALS[province][city]     the portal settings for one city
    PORTALS[...][city]["datasets"][dataset_id]
                                settings for one dataset in that city

resolve_config() merges the three, most specific wins:

    SHARED_CONFIG  <-  city  <-  dataset

Why 'platform' is required
--------------------------
Province and city tell you where the data is. They do not tell you how to read
it, and that is the part that actually changes between portals:

    Vancouver   Opendatasoft Explore v2.1   limit<=100, offset+limit<=10000,
                                            /exports/json for bulk, ODSQL where
    Toronto     CKAN                        datastore_search, different filters
                                            and a different response shape
    Calgary     Socrata                     $limit/$offset/$where, app tokens

So a new city is not just a new base_url. The way you query it changes too. The
platform key picks the adapter that knows those rules, and that keeps
APIDownloader clean. Otherwise it fills up with "if city == Toronto" and we are
back to hardcoded logic.

Why I check the required keys instead of just using .get()
----------------------------------------------------------
I use .get() for anything optional that has a sensible default. That is what it
is good for.

But I do not use it for values the pipeline cannot run without. If I typo
request_timout, .get() hands me None, and requests with timeout=None waits
forever. So the daily job just hangs. It never errors and it never alerts, and
somebody notices three days later that the data is stale. Same shape of problem
as a filter that quietly empties a partition. So the required keys get checked
when we build the config, and the error tells you which one is missing.

Notes on specific values
------------------------
DEFAULT_OUTPUT_ROOT hangs off PROJECT_ROOT, which is the working directory, and
there is exactly one definition of it. In the multi module version this was
Path(__file__).parent.parent because the config lived one directory down, and when
I flattened everything into this file that second definition silently overwrote
the first. Output went to the parent of the working directory and the run still
reported success, reconciled row counts and all. Nothing errored, the files were
just somewhere else. Which is why there is now one definition and it is near the
top.

partition_field is folderyear and not issueddate. 28,720 rows, 14% of the
dataset, have issueddate NULL because they are Pending licences that were never
issued. Filtering "2024 onwards" on issueddate drops every one of them and
nothing tells you. folderyear comes off the licence number so it is populated on
every row.

expected_nullable is measured, not guessed. Address fields are blank for home
based businesses, which the City has withheld since April 2018. country runs 16
to 22% null, city and province about 0.03%, and businessname 6 to 7%, which I
think is sole proprietors that only have a trade name. All of it is structural, so
those columns are profiled rather than alerted on.

strict_natural_key defaults to False because the live partition genuinely contains
duplicate licencersn values. Failing the load would throw away 72k good rows over
4 bad ones, and those 4 resolve themselves once the year closes.

shrink_alert_threshold and null_rate_drift_tolerance both exist for the same
reason: a partition losing 10% of its rows is upstream truncation and should stop
the run, but a column that is 22% null by design will be 22% null tomorrow too. So
volume drops abort and null rates only warn when they move.
"""

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "data"

SHARED_CONFIG: dict[str, Any] = {
    "request_timeout": (10, 300),
    "max_retries": 4,
    "backoff_base_seconds": 2,
    "user_agent": "open-data-pipeline/1.0 (+batch ingestion)",
    "extra_headers": {},

    "output_root": DEFAULT_OUTPUT_ROOT,
    "output_format": "json",

    "shrink_alert_threshold": 0.10,
    "null_rate_drift_tolerance": 0.05,
    "strict_natural_key": False,
}

PORTALS: dict[str, dict[str, dict[str, Any]]] = {
    "BritishColumbia": {
        "Vancouver": {
            "platform": "opendatasoft_v2_1",
            "base_url": "https://opendata.vancouver.ca/api/explore/v2.1",
            "portal_name": "City of Vancouver Open Data Portal",
            "licence": "Open Government Licence - Vancouver",
            "timezone": "America/Vancouver",

            "datasets": {
                "business-licences": {
                    "partition_field": "folderyear",
                    "partition_kind": "two_digit_year",
                    "partition_start_year": 2024,

                    "natural_key": "licencersn",

                    "watermark_field": "extractdate",

                    "expected_columns": {
                        "folderyear", "licencersn", "licencenumber",
                        "licencerevisionnumber", "businessname",
                        "businesstradename", "status", "issueddate", "expireddate",
                        "businesstype", "businesssubtype", "unit", "unittype",
                        "house", "street", "city", "province", "country",
                        "postalcode", "localarea", "numberofemployees", "feepaid",
                        "extractdate", "geom", "geo_point_2d",
                    },
                    "expected_nullable": {
                        "businesstradename", "businesssubtype", "unit", "unittype",
                        "house", "street", "postalcode", "issueddate",
                        "expireddate", "feepaid", "numberofemployees", "geom",
                        "geo_point_2d", "localarea", "businessname", "city",
                        "province", "country",
                    },
                },
            },
        },
    },
}

REQUIRED_KEYS = ("platform", "base_url", "request_timeout", "max_retries")

REQUIRED_DATASET_KEYS = ("partition_field", "natural_key")

def resolve_config(
    province: str,
    city: str,
    dataset_id: str | None = None,
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Merge SHARED_CONFIG <- city <- dataset <- overrides into one flat dict.

    I deep copy it so that if a caller changes the result, it does not leak into
    the next city in the same process. That one is nasty to debug when the
    orchestrator loops over several cities.

    I keep the dataset list on the result as _datasets so we can still discover
    them, but the selected dataset's keys get flattened up to the top. That way
    call sites read one dict instead of walking a tree.
    """
    known_cities = [f"{p}/{c}" for p, cities in PORTALS.items() for c in cities]

    if province not in PORTALS:
        raise ValueError(f"unknown province {province!r}; known: {sorted(PORTALS)}")
    if city not in PORTALS[province]:
        raise ValueError(
            f"unknown city {province}/{city!r}; known: {known_cities}"
        )

    city_config = deepcopy(PORTALS[province][city])
    datasets = city_config.pop("datasets", {})

    merged: dict[str, Any] = {**deepcopy(SHARED_CONFIG), **city_config}
    merged["province"] = province
    merged["city"] = city
    merged["_datasets"] = datasets

    if dataset_id is not None:
        if dataset_id not in datasets:
            raise ValueError(
                f"unknown dataset {dataset_id!r} for {province}/{city}; "
                f"known: {sorted(datasets)}"
            )
        merged.update(deepcopy(datasets[dataset_id]))
        merged["dataset_id"] = dataset_id

    if overrides:
        merged.update({k: v for k, v in overrides.items() if v is not None})

    merged["output_root"] = Path(merged["output_root"])

    missing = [key for key in REQUIRED_KEYS if merged.get(key) is None]
    if dataset_id is not None:
        missing += [key for key in REQUIRED_DATASET_KEYS if merged.get(key) is None]
    if missing:
        raise ValueError(
            f"{province}/{city}{'/' + dataset_id if dataset_id else ''}: "
            f"missing required config key(s): {missing}"
        )

    return merged

# ===============================================================================
# CONFIGURATION - THE CURATED SCHEMA
# ===============================================================================

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

# ===============================================================================
# CONFIGURATION - THE DIMENSIONAL MODEL
# ===============================================================================

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

# ===============================================================================
# TASK 1 - PORTAL CLIENT
# ===============================================================================

"""
Client for the Opendatasoft Explore API v2.1. Vancouver runs it, so do a lot of
other cities.

This file only knows how to talk to one portal API: how to build the urls, how to
write a filter, and where the records sit in the response. It does no HTTP, no
retries and no disk. Those live in open_data_downloader.py.

I split it that way for two reasons. First, it is easy to test against a saved
payload without touching the network. Second, when we add a CKAN or Socrata city
the new file sits next to this one and the downloader does not change at all.

The limits, from their swagger.json, and I confirmed both by making the calls:

    no group_by:        limit <= 100,   offset + limit <= 10000
    with group_by:      limit <= 20000, offset + limit <= 20000
    /exports/{format}:  limit = -1 gives you everything, no offset cap

Their own doc on limit says "If you need more results, please use the /exports
endpoint", and that is right. The licences dataset has about 205k records, so
with /records you can only ever reach the first 10,000. The part that would get
you is it returns 200 the whole way. So a pipeline built on offset paging looks
like it works and quietly gives you 5% of the data.

Aggregate queries like count and max come back as a single row, so the probes
stay well inside the limits.
"""

@dataclass
class PartitionProbe:
    """
    What upstream says about one partition, from a single aggregate query.

    Lives here because parse_probe is what builds it. When a second client shows
    up this should move somewhere shared so the clients are not importing from
    each other, but with one client that would just be an empty file today.
    """
    partition: str
    row_count: int
    watermark: str | None

class OpendatasoftV21Client:
    """Builds the queries and parses the responses for one dataset on one portal."""

    name = "opendatasoft_v2_1"

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        base = config["base_url"].rstrip("/")
        self.dataset_base = f"{base}/catalog/datasets/{config['dataset_id']}"


    def metadata_url(self) -> str:
        return self.dataset_base

    def records_url(self) -> str:
        return f"{self.dataset_base}/records"

    def export_url(self) -> str:
        return f"{self.dataset_base}/exports/{self.config.get('output_format', 'json')}"


    def build_discovery_query(self) -> dict[str, Any]:
        field = self.config["partition_field"]
        return {"select": f"{field},count(*) as n", "group_by": field,
                "order_by": field, "limit": 100}

    def build_probe_query(self, partition: str) -> dict[str, Any]:
        watermark = self.config.get("watermark_field")
        select = "count(*) as n"
        if watermark:
            select += f", max({watermark}) as watermark"
        return {"select": select,
                "where": f'{self.config["partition_field"]}="{partition}"',
                "limit": 1}

    def build_export_query(self, partition: str) -> dict[str, Any]:
        return {"where": f'{self.config["partition_field"]}="{partition}"',
                "limit": -1}


    def parse_metadata(self, payload: Any) -> dict[str, Any]:
        metas = payload.get("metas") or payload.get("dataset", {}).get("metas") or {}
        default = metas.get("default", {})
        return {
            "records_count": default.get("records_count"),
            "modified": default.get("modified"),
            "data_processed": default.get("data_processed"),
        }

    def parse_discovery(self, payload: Any) -> list[str]:
        field = self.config["partition_field"]
        values: list[str] = []
        for row in payload.get("results", []):
            value = row.get(field)
            if value is None:
                log.warning("upstream returned a NULL %s group (%s rows), "
                            "we cannot ingest that so skipping it", field, row.get("n"))
                continue
            values.append(str(value))
        return values

    def parse_probe(self, payload: Any, partition: str) -> PartitionProbe:
        row = (payload.get("results") or [{}])[0]
        return PartitionProbe(partition=partition,
                              row_count=int(row.get("n") or 0),
                              watermark=row.get("watermark"))

    def parse_export(self, payload: Any) -> list[dict[str, Any]]:
        if not isinstance(payload, list):
            raise RuntimeError(
                f"expected a JSON array from the export endpoint, "
                f"got {type(payload).__name__}"
            )
        return payload

# ===============================================================================
# TASK 1 - DOWNLOADER
# ===============================================================================

"""
APIDownloader. Pulls partitioned open data records for a city we have config for.

    downloader = APIDownloader(city="Vancouver", province="BritishColumbia",
                               dataset_id="business-licences")
    partitions = downloader.discover_partitions()      # ['24', '25', '26']
    probe      = downloader.probe("26")                # cheap, count + watermark
    records    = downloader.download("26")             # the actual bulk pull
    path       = downloader.save_records(records, "26")

How this is split up
--------------------
    open_data_configs.py     what to pull
    opendatasoft_client.py   how one portal API works
    open_data_downloader.py  this file: transport and disk

APIDownloader knows nothing about any specific portal. It owns the session,
headers, retries, timeouts and where files land. Everything portal specific goes
to the client, which it picks up from the platform key in config.

I split it that way because province and city tell you where the data is, not how
to read it. Vancouver is Opendatasoft, Toronto is CKAN, Calgary is Socrata, and
they all query differently. If this class knew about all of them directly it would
end up as a pile of if statements per city. With the client split, adding a
Socrata city is a new client file plus one line in CLIENTS, and nothing in here
changes.

There are two request paths on purpose, because there are two questions:

    probe()     did anything change? One aggregate row. Cheap enough to run for
                every partition every day.
    download()  give me the whole partition. Bulk endpoint, no paging.

Keeping them apart is what makes the daily run cheap. On a normal day it is three
probes and zero downloads.
"""

CLIENTS = {
    OpendatasoftV21Client.name: OpendatasoftV21Client,
}

__all__ = ["APIDownloader", "PartitionProbe", "CLIENTS"]

class APIDownloader:
    """Downloads one city and dataset, whatever portal software it runs on."""

    def __init__(
        self,
        city: str,
        province: str,
        dataset_id: str | None = None,
        output_root: str | Path | None = None,
        **overrides: Any,
    ) -> None:
        self.config = resolve_config(
            province, city, dataset_id,
            overrides={"output_root": output_root, **overrides},
        )
        self.city = city
        self.province = province
        self.dataset_id = dataset_id

        platform = self.config["platform"]
        if platform not in CLIENTS:
            raise ValueError(
                f"no client for platform {platform!r}; available: {sorted(CLIENTS)}"
            )
        self.client = CLIENTS[platform](self.config)

        self.session = requests.Session()
        self.session.headers.update(self.get_headers())

    def __repr__(self) -> str:
        return (f"APIDownloader({self.province}/{self.city}, "
                f"dataset={self.dataset_id!r}, platform={self.client.name!r})")


    def get_headers(self) -> dict[str, str]:
        """
        Headers for every request.

        I set a real User-Agent because this is a public portal and it costs us
        nothing. If we ever cause a problem, the operator has someone to contact
        instead of just rate limiting an anonymous client.

        If a portal needs a key, like a Socrata app token, it goes in through
        extra_headers and it comes from the environment. It does not go in the
        config file.
        """
        headers = {
            "Accept": "application/json",
            "User-Agent": self.config.get("user_agent", "open-data-pipeline"),
        }
        headers.update(self.config.get("extra_headers") or {})
        return headers

    def _request(self, url: str, params: dict[str, Any] | None = None) -> Any:
        """
        GET with retries and backoff.

        I only retry things that can actually pass on a second try: connection
        errors, timeouts, 429 and 5xx. Any other 4xx means our query is wrong,
        and retrying a bad query four times just wastes time and hides the real
        problem, so that one fails right away.
        """
        max_retries = self.config["max_retries"]
        backoff = self.config.get("backoff_base_seconds", 2)
        timeout = self.config["request_timeout"]

        last_error: Exception | None = None
        for attempt in range(1, max_retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=timeout)
                if response.status_code == 429 or response.status_code >= 500:
                    raise requests.HTTPError(
                        f"retryable status {response.status_code}", response=response
                    )
                response.raise_for_status()
                return response.json()
            except (requests.ConnectionError, requests.Timeout,
                    requests.HTTPError) as exc:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if status is not None and status < 500 and status != 429:
                    raise
                last_error = exc
                if attempt == max_retries:
                    break
                delay = backoff ** attempt
                log.warning("request failed (attempt %d/%d): %s, retrying in %ds",
                            attempt, max_retries, exc, delay)
                time.sleep(delay)
        raise RuntimeError(f"GET {url} failed after {max_retries} attempts: {last_error}")


    def fetch_metadata(self) -> dict[str, Any]:
        """
        What the publisher says about the dataset.

        I use the record count from here to check against what we actually wrote.
        Comparing our number against their number is cheap and it catches a lot.
        """
        return self.client.parse_metadata(self._request(self.client.metadata_url()))

    def discover_partitions(self) -> list[str]:
        """
        Ask upstream which partitions exist, then keep the ones from our start
        year onwards.

        I do not hardcode the list because in January there will be a folder year
        27 and I do not want that to need a deploy. A new partition should just be
        data.
        """
        payload = self._request(self.client.records_url(),
                                self.client.build_discovery_query())
        values = self.client.parse_discovery(payload)

        start_year = self.config.get("partition_start_year")
        if start_year is None:
            return sorted(values)
        return sorted(v for v in values if self.partition_to_year(v) >= start_year)

    def partition_to_year(self, partition: str) -> int:
        """
        Turn '24' into 2024 when the partition is a two digit year.

        I compare as a number, not a string. A string compare like >= '24' would
        also match '97', which is 1997. That history lives in separate datasets
        today so it would not show up here, but the check is free so I would
        rather have it than find out later.
        """
        if self.config.get("partition_kind") != "two_digit_year":
            return int(partition)
        value = int(str(partition).strip())
        return 2000 + value if value <= 79 else 1900 + value

    def normalise_partition(self, value: str | int) -> str:
        """
        The other direction: take 2024 from a person and give back the '24' the
        API actually wants.

        The two digit folder year is an encoding detail of this portal. Nobody
        typing a command should have to know it, and the folders on disk are
        named 2024 anyway, so asking for --partition 24 to get a folder called
        2024 was just confusing. This accepts either form so old commands and
        scripts keep working.
        """
        text = str(value).strip()
        if self.config.get("partition_kind") != "two_digit_year":
            return text
        return text[-2:] if len(text) == 4 else text.zfill(2)

    def probe(self, partition: str) -> PartitionProbe:
        """
        Row count and watermark for one partition, in one row.

        This is on purpose not the bulk endpoint. If asking "did anything change"
        costs a 50MB download, then the whole daily incremental is pointless.
        """
        payload = self._request(self.client.records_url(),
                                self.client.build_probe_query(partition))
        return self.client.parse_probe(payload, partition)

    def download(self, partition: str) -> list[dict[str, Any]]:
        """Pull one whole partition through the portal's bulk endpoint."""
        log.info("downloading %s partition=%s ...", self.dataset_id, partition)
        payload = self._request(self.client.export_url(),
                                self.client.build_export_query(partition))
        records = self.client.parse_export(payload)
        log.info("downloaded %d records for partition=%s", len(records), partition)
        return records


    def partition_path(self, partition: str) -> Path:
        """
        Where a partition lands:

        <output_root>/<city>/<dataset>/<year>/records.json

        City and dataset are in the path so one landing zone can hold more than
        one portal, and more than one dataset per city, without stepping on
        itself. I use the full year in the folder name rather than the raw '24'
        that comes back from the API, because a human reading the tree should not
        have to know the folder year encoding.

        Note this makes each run overwrite the year's file rather than keeping
        the old one. The writes are still atomic so you never get a half file,
        but we do not keep snapshot history. That is a real trade off: licence
        statuses change after the fact, so with this layout we cannot answer what
        we knew on a given day, only what is true now. If we ever need that, add
        the extract date back as a folder under the year and the writes become
        immutable again.
        """
        return (Path(self.config["output_root"]) / self.city.lower()
                / str(self.dataset_id) / str(self.partition_to_year(partition))
                / "records.json")

    def save_records(self, records: list[dict[str, Any]], partition: str) -> Path:
        """
        Write the partition to disk, exactly as it came back.

        I do not send this through pandas, on purpose. A DataFrame round trip
        turns None into NaN, which is not valid JSON, promotes ints to floats, and
        messes up the nested geom and geo_point_2d objects. If the raw file has
        already been retyped then you cannot use it to check the transform, and
        checking the transform is the whole point of keeping it.

        Written to a temp file first and then moved into place. If you write
        straight to the target and the run dies halfway, a reader picks up half a
        JSON array and reports a row count that looks fine. This way the file
        either exists complete or it does not exist.

        If you want typed output, use save_dataframe() and put it next to this
        one. Not instead of it.
        """
        target = self.partition_path(partition)
        target.parent.mkdir(parents=True, exist_ok=True)

        handle, tmp_name = tempfile.mkstemp(dir=str(target.parent), suffix=".tmp")
        try:
            with os.fdopen(handle, "w", encoding="utf-8") as stream:
                json.dump(records, stream, ensure_ascii=False)
            os.replace(tmp_name, target)
        except BaseException:
            Path(tmp_name).unlink(missing_ok=True)
            raise

        log.info("wrote %d records -> %s", len(records), target)
        return target

    def save_dataframe(
        self,
        frame: pd.DataFrame,
        partition: str,
        fmt: str = "parquet",
    ) -> Path:
        """
        Optional typed copy next to the raw json, for whoever wants to query it.

        Parquet by default because it carries a schema, so the int/float and null
        confusion you get from writing a DataFrame to JSON does not happen. Also
        150MB of JSON is not something you want to query.
        """
        target = self.partition_path(partition).parent / f"records.{fmt}"
        target.parent.mkdir(parents=True, exist_ok=True)
        if fmt == "parquet":
            frame.to_parquet(target, index=False)
        elif fmt == "csv":
            frame.to_csv(target, index=False)
        else:
            raise ValueError(f"unsupported dataframe format {fmt!r}")
        log.info("wrote dataframe (%d rows) -> %s", len(frame), target)
        return target

# ===============================================================================
# TASK 1 - DOWNLOAD ORCHESTRATION
# ===============================================================================

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

# ===============================================================================
# TASK 2 - READER
# ===============================================================================

"""
Reads the raw json the downloader wrote into a DataFrame.

Takes a year so we can transform one partition at a time.

Note there is deliberately NO date filter here. It used to be in this file, which
is the obvious place for it, and it was wrong: filtering before deduplication
hides duplicate primary keys whose two rows fall on different dates. The window
lives at the end of data_transformer instead, after the key is enforced. See the
docstring there.

Nothing is cast here either. The reader's job is to get the records into a frame
with the raw column names intact, so if a cast blows up later you can still see
exactly what came in.
"""

def available_years(raw_root: Path) -> list[int]:
    """Year folders the downloader has written, sorted."""
    if not raw_root.exists():
        return []
    years = []
    for child in raw_root.iterdir():
        if child.is_dir() and child.name.isdigit() and (child / "records.json").exists():
            years.append(int(child.name))
    return sorted(years)

def read_partition(raw_root: Path, year: int) -> pd.DataFrame:
    """One year of raw records, column names untouched."""
    path = raw_root / str(year) / "records.json"
    if not path.exists():
        raise FileNotFoundError(f"no raw file for {year}: {path}")

    with path.open(encoding="utf-8") as stream:
        records = json.load(stream)

    log.info("read %d raw records from %s", len(records), path)
    return pd.DataFrame.from_records(records)

# ===============================================================================
# TASK 2 - COMBINER
# ===============================================================================

"""
Combines the raw year partitions into one frame before anything else happens.

I did not think this stage was needed. One dataset, one portal, so combining
looked like a concat with a nice name. I was wrong, and the reason is worth
writing down because it is not obvious.

The raw layer is partitioned by folderyear, which comes from the licence number.
The curated layer is partitioned by issued_date, because that is the business
date people filter on. Those two do not line up:

    folder year 2024 -> issued dates 2023-11-04 .. 2025-10-06
    folder year 2025 -> issued dates 2024-11-03 .. 2026-04-21
    folder year 2026 -> issued dates 2025-01-01 .. 2026-10-01

A licence for 2026 can be issued in November 2025, which makes sense, you renew
early. So folder years overlap on the calendar: 82 shared days between 2024 and
2025, and 69 between 2025 and 2026.

That means transforming one folder year at a time and writing files named by
issued date is data loss. Transform 2025, it writes 2025/11/2025-11-04.parquet.
Then transform 2026, and it writes the same path with only its own rows. The
2025 rows for that day are gone. No error, no warning, the file is valid parquet
and the row count looks plausible. Same shape as every other bug in this dataset:
the pipeline is green and the answer is wrong.

So everything gets combined first, deduplicated across the whole set, and only
then split by date. It also means the primary key is enforced globally rather
than per partition, which is more correct anyway: licence_rsn identifies a
licence, not a licence within a year.

Cost is that a run always reads every partition, about 205k rows and fifteen
seconds. Worth it.
"""

def combine_years(raw_root, years: list[int]) -> pd.DataFrame:
    """Read the given year partitions and stack them into one frame."""
    frames = []
    for year in years:
        frame = read_partition(raw_root, year)
        if frame.empty:
            log.warning("folder year %s is empty, skipping", year)
            continue
        frames.append(frame)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    log.info("combined %d folder year(s) into %d rows", len(frames), len(combined))
    return combined

# ===============================================================================
# TASK 2 - MANIPULATOR, TYPES
# ===============================================================================

"""
Casts the raw string payloads into the typed schema from configs.

Everything here is driven off COLUMNS, so adding a field is a config change.

The rule I follow: a value that will not cast becomes NA and gets recorded, it
never becomes a guess and it never silently becomes zero. pandas will happily
turn a bad number into NaN and a bad date into NaT, which is the behaviour I
want, but only if the column is a nullable dtype. A plain int64 column cannot
hold NA, so a missing employee count would land as 0 and be indistinguishable
from a real zero. That is why the config uses Int64 and not int64.
"""

def rename_and_select(frame: pd.DataFrame) -> pd.DataFrame:
    """Raw names to curated names, and drop anything not in the schema."""
    present = {raw: name for raw, (name, _) in COLUMNS.items() if raw in frame.columns}
    missing = [raw for raw in COLUMNS if raw not in frame.columns]
    if missing:
        log.warning("raw columns absent from this extract: %s", missing)

    out = frame[list(present)].rename(columns=present)
    for geo in GEO_SOURCE_COLUMNS:
        if geo in frame.columns:
            out[geo] = frame[geo]
    return out

def cast_types(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """
    Apply the schema. Returns the frame plus a count of values lost per column.

    The loss count is the important part. If 40 licence numbers failed to parse
    as integers I want that on the screen, not discovered in a dashboard three
    weeks later.

    Timestamps are parsed with utc=True because issueddate and extractdate both
    carry a +00:00 offset. Without it pandas hands back an object column of mixed
    timezone awareness, and then every later comparison is a coin flip: comparing
    an aware timestamp to a naive one either raises or silently does something you
    did not mean, depending on the pandas version.

    folder_year arrives as the two digit '24'. It gets turned into 2024 so the
    curated layer holds a real year and nobody querying it has to know the
    portal's encoding.
    """
    out = frame.copy()
    losses: dict[str, int] = {}

    for raw_name, (name, dtype) in COLUMNS.items():
        if name not in out.columns:
            continue

        before = out[name].notna().sum()

        if dtype == "timestamp":
            out[name] = pd.to_datetime(out[name], errors="coerce", utc=True, format="ISO8601")
        elif dtype == "date":
            out[name] = pd.to_datetime(out[name], errors="coerce", format="ISO8601").dt.date
        elif dtype in ("Int64", "float64"):
            out[name] = pd.to_numeric(out[name], errors="coerce")
            if dtype == "Int64":
                out[name] = out[name].round().astype("Int64")
        else:
            out[name] = out[name].astype("string")

        after = out[name].notna().sum()
        if after < before:
            losses[name] = int(before - after)

    for name in TWO_DIGIT_YEAR_COLUMNS:
        if name in out.columns:
            two_digit = out[name]
            out[name] = (two_digit + 2000).where(two_digit <= 79, two_digit + 1900)

    for name, lost in losses.items():
        log.warning("cast %s: %d value(s) would not parse, set to NA", name, lost)

    return out, losses

# ===============================================================================
# TASK 2 - MANIPULATOR, TEXT
# ===============================================================================

"""
Cleans up the text columns: whitespace, casing, postal codes.

Every text column gets trimmed and internal whitespace collapsed, because
'Main  St ' and 'Main St' are the same street and should group together.

Casing is where I am deliberately inconsistent, and it is worth explaining.
Geographic and address text gets title cased so it groups. Province and country
are two character codes so they go upper. Business names are left exactly as
they arrive.

That last one is the decision people argue about. Title casing 'MILANO GLOBAL
DEVELOPMENT CORP.' gives you 'Milano Global Development Corp.', which looks
tidier and is wrong: it is not the registered name, and the same rule turns
'ABC Holdings ULC' into 'Abc Holdings Ulc'. A business name is identity, so it
keeps whatever the registry has. For matching and grouping there is a derived
business_name_key, which is uppercased with punctuation stripped. That way you
get a join key without corrupting the real value.
"""

def _clean_whitespace(series: pd.Series) -> pd.Series:
    return (series.astype("string")
                  .str.strip()
                  .str.replace(r"\s+", " ", regex=True)
                  .replace("", pd.NA))

def standardise_text(frame: pd.DataFrame) -> pd.DataFrame:
    """Trim, collapse and case the text columns, and build the name join key."""
    out = frame.copy()

    text_columns = (TITLE_CASE_COLUMNS + UPPER_CASE_COLUMNS
                    + PRESERVE_CASE_COLUMNS + (POSTAL_CODE_COLUMN,)
                    + ("unit", "house"))
    for name in text_columns:
        if name in out.columns:
            out[name] = _clean_whitespace(out[name])

    for name in TITLE_CASE_COLUMNS:
        if name in out.columns:
            out[name] = out[name].str.title()

    for name in UPPER_CASE_COLUMNS:
        if name in out.columns:
            out[name] = out[name].str.upper()

    if NAME_KEY_SOURCE in out.columns:
        out[NAME_KEY_COLUMN] = (
            out[NAME_KEY_SOURCE]
            .str.upper()
            .str.replace(r"[^A-Z0-9 ]", "", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
            .replace("", pd.NA)
        )

    return out

def standardise_postal_codes(frame: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Normalise to 'A1A 1A1' and flag anything that is not a Canadian postal code.

    Returns the frame plus how many failed the pattern.

    I keep the cleaned value either way rather than nulling it. An out of town
    licence holder can legitimately have a US zip or something else entirely, and
    throwing that away to make the column tidy loses real information. The row
    carries a flag instead, so anyone who needs strictly Canadian codes can
    filter on it and anyone who just wants the address still has it.
    """
    out = frame.copy()
    name = POSTAL_CODE_COLUMN
    if name not in out.columns:
        return out, 0

    compact = (out[name].str.upper()
                        .str.replace(r"[^A-Z0-9]", "", regex=True)
                        .replace("", pd.NA))
    six = compact.str.len() == 6
    out[name] = compact.where(~six, compact.str.slice(0, 3) + " " + compact.str.slice(3, 6))

    valid = out[name].str.match(POSTAL_CODE_PATTERN).fillna(False)
    invalid_count = int((out[name].notna() & ~valid).sum())
    out["postal_code_valid"] = valid.where(out[name].notna(), pd.NA).astype("boolean")

    if invalid_count:
        log.warning("%d postal code(s) do not match the Canadian pattern, kept and flagged",
                    invalid_count)
    return out, invalid_count

# ===============================================================================
# TASK 2 - MANIPULATOR, GEO
# ===============================================================================

"""
Turns the nested geo objects into two plain float columns, latitude and longitude.

The raw data carries the same point twice, in two shapes:

    geo_point_2d  {'lon': -123.121054, 'lat': 49.263014}
    geom          {'type': 'Feature',
                   'geometry': {'type': 'Point',
                                'coordinates': [-123.121054, 49.263014]},
                   'properties': {}}

I checked whether they ever disagree and on the rows where both are present they
match exactly, so geo_point_2d is the primary and geom is the fallback.

THE THING TO GET RIGHT: GeoJSON coordinates are [longitude, latitude], in that
order. Not [lat, lon]. Reading them the wrong way round puts Vancouver in the
Indian Ocean, and nothing errors, you just get a map with no pins on it. This is
the same family of bug as reading a UTC price against a local time volume: the
types are fine, the pipeline is green, the numbers are wrong. So the index is
spelled out below and the bounds check exists to catch it if someone edits this
later.

Coordinate strings are handled too. The API gives objects, but a CSV export of
the same dataset gives 'lat, lon' as text, and if we ever point the reader at one
of those I would rather this already worked than find out in production.
"""

_LON_INDEX, _LAT_INDEX = 0, 1

_COORD_STRING = re.compile(
    r"^\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*$"
)

def _from_geo_point(value: object) -> tuple[float | None, float | None]:
    if isinstance(value, dict) and "lat" in value and "lon" in value:
        try:
            return float(value["lat"]), float(value["lon"])
        except (TypeError, ValueError):
            return None, None
    return None, None

def _from_geom(value: object) -> tuple[float | None, float | None]:
    if not isinstance(value, dict):
        return None, None
    geometry = value.get("geometry") if "geometry" in value else value
    if not isinstance(geometry, dict):
        return None, None
    coords = geometry.get("coordinates")
    if not isinstance(coords, (list, tuple)) or len(coords) < 2:
        return None, None
    try:
        return float(coords[_LAT_INDEX]), float(coords[_LON_INDEX])
    except (TypeError, ValueError):
        return None, None

def _from_string(value: object) -> tuple[float | None, float | None]:
    """
    'lat, lon' as text, which is what the CSV export of this dataset gives.

    Note the order flips versus GeoJSON. Opendatasoft writes geo points as
    'lat, lon' in CSV and as [lon, lat] in GeoJSON, which is exactly the kind of
    inconsistency that makes this worth a function rather than an inline lambda.
    """
    if not isinstance(value, str):
        return None, None
    match = _COORD_STRING.match(value)
    if not match:
        return None, None
    return float(match.group(1)), float(match.group(2))

def parse_coordinates(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """
    Add latitude and longitude float columns, drop the nested source columns.

    Returns the frame plus counts of what happened, so the caller can log how
    many rows have no location and how many had a value that is not on Earth.
    """
    out = frame.copy()
    latitudes: list[float | None] = []
    longitudes: list[float | None] = []

    point_col = "geo_point_2d" if "geo_point_2d" in out.columns else None
    geom_col = "geom" if "geom" in out.columns else None

    for _, row in out.iterrows():
        lat = lon = None
        if point_col is not None:
            lat, lon = _from_geo_point(row[point_col])
            if lat is None:
                lat, lon = _from_string(row[point_col])
        if lat is None and geom_col is not None:
            lat, lon = _from_geom(row[geom_col])
            if lat is None:
                lat, lon = _from_string(row[geom_col])
        latitudes.append(lat)
        longitudes.append(lon)

    out["latitude"] = pd.array(latitudes, dtype="float64")
    out["longitude"] = pd.array(longitudes, dtype="float64")

    has_point = out["latitude"].notna() & out["longitude"].notna()

    lat_lo, lat_hi = LATITUDE_BOUNDS
    lon_lo, lon_hi = LONGITUDE_BOUNDS
    off_earth = has_point & ~(out["latitude"].between(lat_lo, lat_hi)
                              & out["longitude"].between(lon_lo, lon_hi))

    bc = BC_BOUNDING_BOX
    outside_bc = has_point & ~off_earth & ~(
        out["latitude"].between(*bc["lat"]) & out["longitude"].between(*bc["lon"])
    )

    out["has_coordinates"] = has_point.astype("boolean")
    out = out.drop(columns=[c for c in GEO_SOURCE_COLUMNS if c in out.columns])

    counts = {
        "with_coordinates": int(has_point.sum()),
        "without_coordinates": int((~has_point).sum()),
        "off_earth": int(off_earth.sum()),
        "outside_bc": int(outside_bc.sum()),
    }

    log.info("coordinates: %d parsed, %d without a point",
             counts["with_coordinates"], counts["without_coordinates"])
    if counts["off_earth"]:
        log.warning("%d row(s) have coordinates outside valid lat/lon bounds",
                    counts["off_earth"])
    if counts["outside_bc"]:
        log.info("%d row(s) plot outside BC, expected for out of town addresses",
                 counts["outside_bc"])

    return out, counts

# ===============================================================================
# TASK 2 - DATA QUALITY
# ===============================================================================

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
        record(out["status"].notna() & ~out["status"].isin(VALID_STATUSES),
               "unknown_status")

    if "postal_code_valid" in out.columns:
        record(out["postal_code_valid"].eq(False), "postal_code_not_canadian")

    if {"latitude", "longitude"} <= set(out.columns):
        has_point = out["latitude"].notna() & out["longitude"].notna()
        lat_lo, lat_hi = LATITUDE_BOUNDS
        lon_lo, lon_hi = LONGITUDE_BOUNDS
        record(has_point & ~(out["latitude"].between(lat_lo, lat_hi)
                             & out["longitude"].between(lon_lo, lon_hi)),
               "coordinates_out_of_bounds")

    for name in REQUIRED_NOT_NULL:
        if name in out.columns and name != PRIMARY_KEY:
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
    key = PRIMARY_KEY
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
    sort_columns = [c for c, _ in DEDUPE_ORDER if c in out.columns]
    ascending = [asc for c, asc in DEDUPE_ORDER if c in out.columns]

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

# ===============================================================================
# TASK 2 - EXPORTER
# ===============================================================================

"""
Writes the curated frame out as parquet, one file per issued date.

    curated/<city>/<dataset>/<year>/<month>/<YYYY-MM-DD>.parquet
    curated/<city>/<dataset>/<year>/<month>/<YYYY-MM-DD>.quarantine.parquet
    curated/<city>/<dataset>/<year>/unknown/no-issued-date.parquet

Year and month come from issued_date, because that is the business date people
filter on. A daily run writes one file, a backfill writes one per day it covers,
and re-running a day replaces exactly that file and touches nothing else. That
last part is the real reason for this layout: the raw layer overwrites a whole
year at a time, and here a bad day costs one file instead of the year.

Parquet rather than json because it carries the schema. That is the point of this
whole layer: raw is untyped strings, and if the output were json again every
reader would have to re-guess the types and they would not all guess the same.

Rows with no issued_date go to <folder_year>/unknown/. There are 11,877 of them
in 2026, they are Pending licences that were never issued, and they are real
records. Putting them in a named folder keeps them queryable and obvious. The
alternative, quietly dropping anything that cannot be placed on a calendar, is
the same bug the downloader avoids by partitioning on folderyear.

Quarantine sits next to the day it came from rather than in its own tree, so
anyone looking at a day's output trips over the rejects instead of having to know
they exist.
"""

UNKNOWN_DATE_DIR = "unknown"

UNKNOWN_DATE_FILE = "no-issued-date"

def _atomic_to_parquet(frame: pd.DataFrame, target: Path) -> None:
    """Temp file then os.replace, so a reader never sees a half written file."""
    target.parent.mkdir(parents=True, exist_ok=True)
    handle, tmp_name = tempfile.mkstemp(dir=str(target.parent), suffix=".tmp")
    os.close(handle)
    try:
        frame.to_parquet(tmp_name, index=False)
        os.replace(tmp_name, target)
    except BaseException:
        Path(tmp_name).unlink(missing_ok=True)
        raise

def _day_keys(frame: pd.DataFrame) -> pd.DataFrame:
    """
    Work out the year / month / file name for every row.

    Falls back to folder_year with an 'unknown' month when issued_date is null,
    so those rows still land somewhere deterministic instead of being skipped.
    """
    issued = pd.to_datetime(frame.get("issued_date"), errors="coerce", utc=True)
    undated = issued.isna()

    keys = pd.DataFrame(index=frame.index)
    keys["year"] = issued.dt.year.astype("Int64")
    keys["month"] = issued.dt.strftime("%m")
    keys["name"] = issued.dt.strftime("%Y-%m-%d")

    if undated.any():
        if "folder_year" in frame.columns:
            keys.loc[undated, "year"] = frame.loc[undated, "folder_year"]
        keys.loc[undated, "month"] = UNKNOWN_DATE_DIR
        keys.loc[undated, "name"] = UNKNOWN_DATE_FILE

    return keys

def write_daily_files(
    frame: pd.DataFrame,
    rejected: pd.DataFrame,
    curated_root: Path,
    city: str,
    dataset: str,
) -> dict[str, int]:
    """
    Split both frames by issued date and write a parquet file per day.

    Returns counts of what was written so the caller can log it.
    """
    base = Path(curated_root) / city.lower() / dataset
    written = {"curated_files": 0, "curated_rows": 0,
               "quarantine_files": 0, "quarantine_rows": 0}

    if len(frame):
        keys = _day_keys(frame)
        for (year, month, name), group in frame.groupby(
                [keys["year"], keys["month"], keys["name"]], dropna=False):
            if pd.isna(year):
                target = base / UNKNOWN_DATE_DIR / UNKNOWN_DATE_DIR / f"{UNKNOWN_DATE_FILE}.parquet"
            else:
                target = base / str(int(year)) / str(month) / f"{name}.parquet"
            _atomic_to_parquet(group, target)
            written["curated_files"] += 1
            written["curated_rows"] += len(group)
            log.debug("wrote %d rows -> %s", len(group), target)

    if len(rejected):
        keys = _day_keys(rejected)
        for (year, month, name), group in rejected.groupby(
                [keys["year"], keys["month"], keys["name"]], dropna=False):
            if pd.isna(year):
                target = (base / UNKNOWN_DATE_DIR / UNKNOWN_DATE_DIR
                          / f"{UNKNOWN_DATE_FILE}.quarantine.parquet")
            else:
                target = base / str(int(year)) / str(month) / f"{name}.quarantine.parquet"
            _atomic_to_parquet(group, target)
            written["quarantine_files"] += 1
            written["quarantine_rows"] += len(group)
            log.warning("quarantined %d row(s) -> %s", len(group), target)

    log.info("wrote %d curated file(s) covering %d rows%s",
             written["curated_files"], written["curated_rows"],
             f", plus {written['quarantine_files']} quarantine file(s)"
             if written["quarantine_files"] else "")
    return written

# ===============================================================================
# TASK 2 - TRANSFORM ORCHESTRATION
# ===============================================================================

"""
Runs the stages in order over every raw partition at once.

    combine -> cast -> standardise text -> parse geo -> checks -> primary key
            -> date window -> export by issued date

Each stage is its own module and each one takes a frame and gives back a frame,
so you can run them one at a time in a notebook when something looks wrong. That
is most of why they are split at all.

Why everything is combined first
--------------------------------
The raw layer is partitioned by folderyear, which comes off the licence number.
The curated layer is partitioned by issued_date, which is the date people
actually filter on. Those do not line up, because a 2026 licence can be issued in
November 2025. Folder years overlap on the calendar by 82 days between 2024 and
2025, and 69 days between 2025 and 2026.

So transforming one folder year at a time and writing files named by issued date
loses data: the second year writes the same day file and the first year's rows for
that day are gone, with no error and a plausible row count. Combining first fixes
it, and it also makes the primary key global, which is what licence_rsn actually
is. See combiner/partition_combiner.py.

Why the date window is applied LAST
-----------------------------------
Same class of problem. My first version filtered by date in the reader, which is
the obvious place: read less, work less. It hides duplicate keys whose two rows
sit on different dates. The full 2026 partition has 4 duplicate licence_rsn
values, but a one week window only found 3, because the fourth pair straddled the
boundary. So uniqueness is enforced across everything and the window only decides
what gets written.

Undated rows work the same way. 11,874 rows have no issued_date because the
licence is Pending. They go through every stage and land under <year>/unknown/,
but a windowed run does not rewrite them every day unless you ask for them.
"""

def _apply_window(
    frame: pd.DataFrame,
    since: str | None,
    until: str | None,
    include_undated: bool,
) -> pd.DataFrame:
    """
    Narrow to an issued_date window, inclusive on both ends, on the typed column.

    Undated rows are excluded when a window is given, because a Pending licence
    with no issued date does not belong to any particular week and rewriting all
    of them on every daily run is pointless. Pass include_undated to keep them. A
    run with no window keeps everything.

    Either way the excluded count is logged. Silence is what makes this kind of
    filter dangerous.
    """
    if not since and not until:
        return frame

    issued = frame["issued_date"]
    window = pd.Series(True, index=frame.index)
    if since:
        window &= issued >= pd.Timestamp(since, tz="UTC")
    if until:
        window &= issued < pd.Timestamp(until, tz="UTC") + pd.Timedelta(days=1)
    window = window.fillna(False) & issued.notna()

    undated = issued.isna()
    if include_undated:
        window |= undated

    out = frame.loc[window]
    log.info("window %s..%s kept %d of %d rows (%d undated %s)",
             since or "-", until or "-", len(out), len(frame), int(undated.sum()),
             "included" if include_undated else "excluded")
    return out

def transform(
    raw_root: Path,
    curated_root: Path,
    city: str,
    dataset: str,
    years: list[int],
    now: pd.Timestamp,
    since: str | None = None,
    until: str | None = None,
    include_undated: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Everything through the whole chain. Returns a summary for the caller to log.

    `now` comes in from outside so the future date check is reproducible instead
    of depending on when the job happened to run.
    """
    raw = combine_years(raw_root, years)
    if raw.empty:
        log.warning("no raw rows for years %s, nothing to do", years)
        return {"years": years, "curated_rows": 0, "rejected_rows": 0,
                "flagged_rows": 0, "flags": {}, "reject_reasons": {}}

    frame = rename_and_select(raw)
    frame, cast_losses = cast_types(frame)

    frame = standardise_text(frame)
    frame, invalid_postal = standardise_postal_codes(frame)

    frame, geo_counts = parse_coordinates(frame)

    frame, flag_counts = run_checks(frame, now=now)

    frame, rejected = enforce_primary_key(frame)

    duplicates = int(frame[PRIMARY_KEY].duplicated().sum())
    if duplicates:
        raise AssertionError(
            f"{PRIMARY_KEY} still has {duplicates} duplicate(s) after "
            "enforcement, refusing to write"
        )

    frame = _apply_window(frame, since, until, include_undated)

    if len(rejected):
        log.info("quarantine covers everything read: %d row(s)", len(rejected))

    summary: dict[str, Any] = {
        "years": years,
        **summarise(frame, rejected),
        "flags_before_window": dict(sorted(flag_counts.items(), key=lambda kv: -kv[1])),
        "cast_losses": cast_losses,
        "invalid_postal_codes": invalid_postal,
        "coordinates": geo_counts,
        "columns": len(frame.columns),
    }

    if dry_run:
        log.info("dry run, not writing")
        return summary

    summary["written"] = write_daily_files(
        frame, rejected, curated_root, city, dataset
    )
    return summary

# ===============================================================================
# TASK 3 - DIMENSIONS
# ===============================================================================

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

# ===============================================================================
# TASK 3 - FACTS
# ===============================================================================

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

# ===============================================================================
# TASK 3 - TEMPORAL
# ===============================================================================

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

# ===============================================================================
# TASK 3 - THE THREE QUESTIONS
# ===============================================================================

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

# ===============================================================================
# TASK 3 - MODEL ORCHESTRATION
# ===============================================================================

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
    curated = business_identity(curated)

    earliest_folder_year = int(pd.to_numeric(curated["folder_year"],
                                             errors="coerce").min())
    log.info("earliest folder year in the data is %d, so lifespan is left "
             "censored at that boundary", earliest_folder_year)

    dim_address, address_sk = build_dim_address(curated)
    dim_neighbourhood, neighbourhood_sk = build_dim_neighbourhood(curated)
    dim_category, category_sk = build_dim_category(curated)
    dim_status, status_sk = build_dim_status(curated)

    tables: dict[str, pd.DataFrame] = {
        "dim_business": build_dim_business(curated),
        "dim_address": dim_address,
        "dim_neighbourhood": dim_neighbourhood,
        "dim_category": dim_category,
        "dim_status": dim_status,
        "dim_date": build_dim_date(curated),
    }

    fact_licence = build_fact_licence(
        curated, address_sk, neighbourhood_sk, category_sk, status_sk, as_of)
    tables["fact_licence"] = fact_licence
    tables["fact_business_lifecycle"] = build_fact_business_lifecycle(
        fact_licence, earliest_folder_year)
    address_change, relocation_counts = build_fact_address_change(
        fact_licence, dim_address)
    tables["fact_address_change"] = address_change
    tables["dim_business_history"] = build_dim_business_history(
        curated, fact_licence)
    tables["fact_status_transition"] = build_fact_status_transition(fact_licence)
    tables["fact_renewal"] = build_fact_renewal(fact_licence)

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
# ===============================================================================
# COMMAND LINE
# ===============================================================================

def main(argv: list[str] | None = None) -> int:
    """
    Runs the stages in order, or one of them on its own.

    Each stage is independent enough to run alone as long as the one before it has
    produced output. Download writes raw JSON, transform reads it and writes
    curated parquet, model reads that and writes the star schema. So a rebuild of
    just the model does not re-download 200k rows.

    --since and --until only affect the transform, which is where a date window
    belongs. The download works on whole folder years because that is the grain
    the portal publishes, and the model needs everything at once because folder
    years overlap on the calendar.
    """
    parser = argparse.ArgumentParser(
        description="City of Vancouver business licences: download, transform, model.")
    parser.add_argument("--stage", choices=("download", "transform", "model", "all"),
                        default="all")
    parser.add_argument("--city", default="Vancouver")
    parser.add_argument("--province", default="BritishColumbia")
    parser.add_argument("--dataset", default="business-licences")
    parser.add_argument("--partition", action="append", dest="partitions",
                        metavar="YEAR", help="download only these year(s), e.g. 2026")
    parser.add_argument("--since", metavar="YYYY-MM-DD",
                        help="transform only rows issued on or after this date")
    parser.add_argument("--until", metavar="YYYY-MM-DD",
                        help="transform only rows issued on or before this date")
    parser.add_argument("--include-undated", action="store_true",
                        help="with a date window, also write the Pending rows")
    parser.add_argument("--start", metavar="YYYY-MM-DD",
                        help="start of the range for question 1")
    parser.add_argument("--end", metavar="YYYY-MM-DD",
                        help="end of the range for question 1")
    parser.add_argument("--all-closed", action="store_true",
                        help="question 2 over every closed business, which is biased")
    parser.add_argument("--full-refresh", action="store_true",
                        help="re-download every partition")
    parser.add_argument("--strict", action="store_true",
                        help="duplicate natural keys become fatal")
    parser.add_argument("--dry-run", action="store_true", help="write nothing")
    parser.add_argument("--output-root", default=None)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(message)s")

    root = Path(args.output_root) if args.output_root else PROJECT_ROOT
    raw_root = root / "data"
    curated_root = root / "curated"
    model_root = root / "model"
    now = pd.Timestamp(datetime.now(timezone.utc))

    try:
        if args.stage in ("download", "all"):
            code = run(city=args.city, province=args.province, dataset_id=args.dataset,
                       partitions=args.partitions, full_refresh=args.full_refresh,
                       dry_run=args.dry_run, strict=args.strict,
                       output_root=str(raw_root), run_transform=False)
            if code:
                return code

        if args.stage in ("transform", "all"):
            dataset_raw = raw_root / args.city.lower() / args.dataset
            years = available_years(dataset_raw)
            if not years:
                log.error("no raw partitions under %s, run the download stage first",
                          dataset_raw)
                return 1
            summary = transform(raw_root=dataset_raw, curated_root=curated_root,
                                city=args.city.lower(), dataset=args.dataset,
                                years=years, now=now, since=args.since,
                                until=args.until,
                                include_undated=args.include_undated,
                                dry_run=args.dry_run)
            log.info("transform: curated=%d rejected=%d flagged=%d",
                     summary["curated_rows"], summary["rejected_rows"],
                     summary["flagged_rows"])
            if summary["flags"]:
                log.info("  flags: %s", summary["flags"])
            if summary["reject_reasons"]:
                log.warning("  rejects: %s", summary["reject_reasons"])

        if args.stage in ("model", "all"):
            if args.dry_run:
                log.info("dry run, skipping the model stage")
                return 0
            counts = build_model(curated_root=curated_root, model_root=model_root,
                                 city=args.city.lower(), dataset=args.dataset,
                                 as_of=now, dry_run=False)
            log.info("model built: %s", counts)

            fact_licence = pd.read_parquet(model_root / "fact_licence.parquet")
            lifecycle = pd.read_parquet(model_root / "fact_business_lifecycle.parquet")
            dim_neighbourhood = pd.read_parquet(model_root / "dim_neighbourhood.parquet")
            dim_category = pd.read_parquet(model_root / "dim_category.parquet")
            dim_business = pd.read_parquet(model_root / "dim_business.parquet")
            dim_address = pd.read_parquet(model_root / "dim_address.parquet")
            address_change = pd.read_parquet(model_root / "fact_address_change.parquet")

            start = pd.Timestamp(args.start, tz="UTC") if args.start \
                else fact_licence["issued_date"].min()
            end = pd.Timestamp(args.end, tz="UTC") if args.end else now

            marts = model_root / "marts"
            marts.mkdir(parents=True, exist_ok=True)
            active_vs_closed_by_neighbourhood(fact_licence, dim_neighbourhood,
                                              start, end) \
                .to_parquet(marts / "mart_neighbourhood_activity.parquet", index=False)
            lifespan_by_category(lifecycle, dim_category,
                                 fully_observed_only=not args.all_closed) \
                .to_parquet(marts / "mart_lifespan_by_category.parquet", index=False)
            relocations = address_changes(address_change, dim_business, dim_address)
            if not relocations.empty:
                relocations.to_parquet(marts / "mart_address_changes.parquet", index=False)
            log.info("marts written to %s", marts)

    except FileNotFoundError as exc:
        log.error("%s", exc)
        return 1
    except ValueError as exc:
        log.error("configuration problem: %s", exc)
        return 2
    except AssertionError as exc:
        log.error("integrity check failed: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
