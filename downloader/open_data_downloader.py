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

from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from downloader.open_data_configs import resolve_config
from downloader.opendatasoft_client import OpendatasoftV21Client, PartitionProbe

log = logging.getLogger(__name__)

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
