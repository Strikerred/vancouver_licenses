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
DEFAULT_OUTPUT_ROOT uses parent.parent, not parent. This file lives in
downloader/, and the data belongs next to the project rather than inside the
package. I got that wrong when I moved the module into the package and the only
symptom was that it re-downloaded everything into downloader/data and still
reported success. Nothing errored, the row counts even reconciled, the files were
just in the wrong place.

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

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
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
