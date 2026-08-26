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

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

log = logging.getLogger(__name__)


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
