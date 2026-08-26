# Open data pipeline — City of Vancouver business licences

Two stage daily batch pipeline. Stage one pulls business licence records (2024
onwards) from the City of Vancouver open data portal into raw JSON. Stage two
casts, checks and writes them out as a typed parquet layer you can trust.

Vancouver is the dataset I was asked for, but the config is built so another city
is a config entry and not a code change.

## Layout

Two packages, downloader and transformer, with one module per source family plus
its own `_configs`. The API client lives in its own module next to the downloader
that uses it, so the code that knows one portal's query syntax is separate from
the code that owns sessions, retries and disk.

| File | What it does |
|---|---|
| **downloader** | |
| `open_data_configs.py` | **what** to pull. `SHARED_CONFIG` plus `PORTALS[province][city][datasets]` |
| `opendatasoft_client.py` | **how one portal API works**. Builds queries, parses responses. No HTTP, no disk |
| `open_data_downloader.py` | **transport and disk**. Session, retries, timeouts, atomic writes |
| **transformer** | |
| `configs/business_licences_configs.py` | the schema, the primary key, the tie break order, the check thresholds |
| `reader/json_reader.py` | read one year of raw json |
| `combiner/partition_combiner.py` | stack the year partitions into one frame |
| `manipulator/type_caster.py` | raw strings into the typed schema |
| `manipulator/text_standardiser.py` | whitespace, casing, postal codes |
| `manipulator/geo_parser.py` | nested geojson into latitude and longitude |
| `util/quality.py` | data quality checks, primary key enforcement, quarantine |
| `exporter/parquet_exporter.py` | parquet out, one file per issued date |
| `data_transformer.py` | the orchestrator |
| **entry points** | |
| `pipeline.py` | download then transform, this is the daily job |
| `transform.py` | transform only, for rebuilds and for working a week at a time |

A new portal type (CKAN for Toronto, Socrata for Calgary) is a new client file
next to `opendatasoft_client.py` plus one line in `CLIENTS`. Nothing in the
downloader changes.

## Running it

```bash
pip install -r requirements.txt

# the daily job: download what moved, then transform
python pipeline.py
python pipeline.py --no-transform            # download only
python pipeline.py --full-refresh            # re-pull everything
python pipeline.py --partition 2026          # just one year
python pipeline.py --dry-run                 # probe only, write nothing

# transform on its own
python transform.py                          # every year on disk
python transform.py --year 2026
python transform.py --since 2026-08-17 --until 2026-08-23   # one week
python transform.py --include-undated        # also rewrite the Pending rows
python transform.py --dry-run                # run the checks, write nothing
```

Exit codes: `0` clean, `1` something failed, `2` bad config or a year that is not
upstream.

`--partition` and `--year` take the year as you would say it, `2026`. The two
digit `folderyear` the API uses is an encoding detail of the portal, so it stays
inside the client.

## Stage 1 — raw

```
data/vancouver/business-licences/2024|2025|2026/records.json
```

205,105 rows, which matches the publisher's own `records_count` exactly. The
pipeline reconciles those two numbers every run and puts it in the summary line,
because there is no manifest file so the log is the audit trail.

Records land exactly as they came back. No renaming, no retyping, no dropping
columns. I deliberately do not put the raw output through pandas, because a
DataFrame round trip turns `None` into `NaN` (not valid JSON), promotes ints to
floats, and mangles the nested `geom` objects. If raw has already been retyped you
cannot use it to check the transform, which is the whole point of keeping it.

### What I found in the API before writing anything

I checked all of this against the live API. Where the docs and the data
disagreed, I went with the data.

| What I found | What I did about it |
|---|---|
| `/records` caps out at `limit<=100` and `offset+limit<=10000`, and the dataset has 205,105 rows | You can only ever see the first 10,000, and it returns 200 the whole way. So offset paging gives you 5% of the data and looks fine. Used `/exports/json?limit=-1`, which is also what their own swagger tells you to do |
| 28,720 rows (14%) have `issueddate` NULL, all Pending plus some Cancelled and Inactive | Filtering "issued from 2024 onwards" on `issueddate` silently drops all of them. Partitioned on `folderyear` instead, it is filled in on every row |
| `extractdate` is the same for every row in a folder year | It is a batch stamp, not a per row change token. No row level delta exists in this API, so the incremental works per partition |
| Only the current folder year refreshes daily | Matches their "Daily (for current year extracts)" note. A normal day is one download and a few cheap probes |
| `licencersn` is unique in 2025 but not in 2026, where 4 rows break it | Warn and land it as received. Picking a winner is a transform decision, see below |
| `numberofemployees` is a double, and their doc says `0 = none, 000 = unknown` | Both end up as `0.0`, so real zero and unknown are the same value. You cannot average this column |
| `status` in the data is `Gone Out of Business`, the field description calls it `GOB` | Code against the data, not the description |

### The incremental

```
for each partition (discovered, not hardcoded):
    probe                -> count(*) and max(extractdate), one row, cheap
    compare against disk -> row count and max extractdate of the file we have
    moved? -> download, validate, write.   same? -> skip
```

**There is no state file.** The data on disk is the state. I read the row count
and the max `extractdate` out of the file we wrote last time. Better than a
sidecar because the two can never disagree, and with a state file you eventually
end up debugging your own bookkeeping instead of the data. Costs a second or two
of parsing per partition.

| Case | Result |
|---|---|
| Cold start | 3 written, 205,105 rows, reconciles |
| Nothing changed | 3 skipped in 3.1s, zero downloads |
| One year deleted | Only that year re-pulled |
| One year corrupted | Detected on parse, warned, re-pulled |

**What it cannot do:** intra day history. If a status changes twice between two
extracts we only see the final one. That is the source, not the pipeline.

## Stage 2 — curated

```
curated/vancouver/business-licences/<year>/<month>/<YYYY-MM-DD>.parquet
curated/vancouver/business-licences/<year>/<month>/<YYYY-MM-DD>.quarantine.parquet
curated/vancouver/business-licences/<year>/unknown/no-issued-date.parquet
```

1,020 daily files, 205,101 rows, `licence_rsn` unique. That is 205,105 raw minus
4 rejected duplicates.

Year and month come from `issued_date`, because that is the business date people
filter on. One file per day means a daily run writes one file and re-running a day
replaces exactly that file, unlike the raw layer which rewrites a whole year.

### Types, verified on disk with pyarrow

| Column | Parquet type |
|---|---|
| `licence_rsn` (primary key) | `int64` |
| `issued_date`, `extract_date` | `timestamp[us, tz=UTC]` |
| `expired_date` | `date32[day]` |
| `latitude`, `longitude` | `double` |
| `fee_paid` | `double` |
| `number_of_employees`, `folder_year`, `licence_revision_number` | `int64` |
| `postal_code_valid`, `has_coordinates` | `bool` |

Raw names are squashed together (`licencersn`), curated names are snake_case
(`licence_rsn`). The curated layer is what people query and it should not inherit
the portal's spelling habits.

Anything that will not cast becomes NA and the count is logged. Never a guess,
never a silent zero. The nullable dtypes (`Int64`, not `int64`) matter here: a
plain `int64` cannot hold NA, so a missing employee count would land as `0` and be
indistinguishable from a real zero.

### Casing

Deliberately inconsistent, and worth explaining. `city`, `street`, `local_area`
and `unit_type` get title cased so they group. `province` and `country` are two
character codes so they go upper. **Business names are left exactly as they
arrive.** Title casing `MILANO GLOBAL DEVELOPMENT CORP.` gives you something
tidier and wrong, and the same rule turns `ABC Holdings ULC` into
`Abc Holdings Ulc`. A name is identity. For matching there is a derived
`business_name_key`, uppercased with punctuation stripped, so you get a join key
without corrupting the real value.

### Geo

`geo_point_2d` is the primary source and `geom` the fallback. I checked and on the
rows where both exist they match exactly.

**GeoJSON coordinates are `[longitude, latitude]`, in that order.** Reading them
the wrong way round puts Vancouver in the Indian Ocean and nothing errors, you
just get a map with no pins. Same family as reading a UTC price against a local
time volume. The index is spelled out in the code and the bounds check exists to
catch it if someone edits it later. Coordinate strings are handled too, because
the CSV export of this dataset writes `lat, lon` while the GeoJSON writes
`[lon, lat]`.

103,072 rows have coordinates. Having none is normal, not a problem: home based
addresses are withheld and out of town addresses were never geocoded.

### Primary key and the tie break

`licence_rsn`, enforced not assumed. The transform refuses to write if the key is
still not unique afterwards.

The 4 duplicates are not duplicate rows, they are two versions of the same licence
caught mid update. The tie break is most complete row first (fewest nulls), then
newest extract, then highest revision. The last key is only there so two runs over
the same input always give the same answer.

It picks correctly. For `4940392` it keeps `Bard on the Beach Theatre Society`
with a full address and quarantines `Bard on the Beach` with every address field
null.

### Data quality

Two severities, because "invalid" covers two different situations.

**Reject** — not a usable record, goes to `*.quarantine.parquet` and stays out of
the curated table: null or non numeric primary key, or losing a key tie break.

**Flag** — usable but worth knowing, stays in the table with the reason in
`dq_flags`:

| Flag | Rows |
|---|---|
| `postal_code_not_canadian` | 150 |
| `expired_before_issued` | 94 |
| `future_issued_date` | 18 |

Flagging rather than dropping is deliberate. The 18 future dated rows are all
status `Issued` and all dated the 1st of a coming month, so they are almost
certainly scheduled issuances rather than corruption. Drop them and the curated
count silently disagrees with the source.

Quarantine sits next to the day it came from rather than in its own tree, so
anyone looking at a day's output trips over the rejects instead of having to know
they exist. It is also **not** windowed: a rejected row is a fact about the data,
not about the week we happen to be writing.

The 28,717 rows with no `issued_date` go to `<year>/unknown/`. They are Pending
licences and they are real records, so they get a named folder rather than being
quietly dropped.

## Handling strategy

Every condition I check for, what happens to the row, and why. The two things I
refuse to do are drop a row silently and let a bad row through unmarked.

| Condition | Severity | What happens to the row | Where it ends up |
|---|---|---|---|
| `/records` paging cap | n/a | avoided by design, `/exports/json` instead | — |
| Empty extract from the API | abort | nothing is written, run exits non zero | raw stays as it was |
| Partition shrank >10% vs what we hold | abort | refuse to overwrite, exit non zero | raw stays as it was |
| Unknown column appeared upstream | abort | refuse to ingest until the contract is reviewed | raw stays as it was |
| Rows carrying the wrong `folderyear` | abort | the upstream filter did not hold, so nothing is trusted | raw stays as it was |
| Raw file missing or corrupt locally | recover | treat the partition as new and re-download | raw rewritten |
| Value will not cast to its type | **flag** | set to NA, count logged per column | curated, NA in that column |
| `licence_rsn` null or non numeric | **reject** | removed from curated | `*.quarantine.parquet` |
| `licence_rsn` duplicated | **reject** the loser | tie break keeps one, the other is removed | `*.quarantine.parquet` |
| `licence_rsn` still duplicated after the tie break | abort | raise, refuse to write a table claiming a key it does not have | nothing written |
| Future dated `issued_date` | **flag** | kept | curated, `dq_flags=future_issued_date` |
| `expired_date` before `issued_date` | **flag** | kept | curated, `dq_flags=expired_before_issued` |
| Status not one of the five documented | **flag** | kept | curated, `dq_flags=unknown_status` |
| Postal code not Canadian format | **flag** | cleaned value kept, not nulled | curated, `postal_code_valid=false` |
| Coordinates outside valid lat/lon | **flag** | kept | curated, `dq_flags=coordinates_out_of_bounds` |
| No coordinates at all | ignore | kept, `has_coordinates=false` | curated, not flagged |
| Null in a required non-key column | **flag** | kept | curated, `dq_flags=null_<column>` |
| No `issued_date` (28,717 Pending rows) | route | kept, cannot be placed on a calendar | `<year>/unknown/` |

Three principles behind that:

**Reject means quarantined, never deleted.** A rejected row goes to parquet next
to the day it came from, with the reason in `dq_reject_reason`. A log line gets
rotated away; a parquet file can be queried in a month when somebody asks what
happened to a licence.

**Flag rather than drop wherever the row is still a real record.** Dropping is
destructive and it hides the problem, and the counts stop matching the source with
no explanation.

**Abort rather than write something wrong.** If the extract itself cannot be
trusted, no output at all is better than plausible bad output, because plausible
bad output gets used.

And one thing I deliberately do not do: alert on conditions that are normal here.
Missing addresses, missing coordinates and 22% null `country` are all structural,
so they are profiled and not warned about. Null rates only warn when they *move*
more than 5%. A pipeline that shouts every day about something normal is a
pipeline whose warnings nobody reads.

## Assumptions

Things I could not verify from the API and had to decide. Each one is a place this
would need revisiting if it turns out to be wrong.

**`folderyear` is the year of issue.** The publisher documents it as the first two
characters of the licence number, representing the year issued, and I use it as
the partition key for "2024 onwards". If it is really an administrative year
rather than an issue year then the raw scope is subtly wrong. Some evidence it is
at least not the calendar year of `issued_date`: folder year 2024 contains rows
issued as early as 2023-11-04.

**Timestamps really are UTC.** `issueddate` and `extractdate` arrive with a
`+00:00` offset and I parse them as UTC. If the publisher is actually writing
Vancouver local time and labelling it `+00:00`, every timestamp in the curated
layer is 7 or 8 hours out and nothing would reveal it. I cannot check this from
the API alone. It is the assumption I would want confirmed first, because it is
both invisible and pervasive — the same shape as the timezone defect I have hit in
production before.

**`licence_rsn` is meant to be unique and the collisions are transient.** It is
unique across all 69,889 rows of the closed 2025 partition and broken by 4 rows in
the live 2026 one, so I treat those as records caught mid update rather than as
the key being unreliable. If collisions ever appear in a closed year, the key
choice needs rethinking.

**The more complete row wins a tie.** When a key collides I keep the row with
fewest nulls. That is a judgment call, not something the City tells us. It gives
the right answer on all 4 cases here, but if the City's real rule is "latest
revision wins regardless" then some of these are backwards.

**`extractdate` moving means the partition was republished.** The whole incremental
rests on it. I hedge by also comparing row counts, so a republish that changed
content without moving the stamp still gets caught, but a republish that changed
neither would be invisible.

**Future dated issuances are scheduled, not errors.** All 18 are status `Issued`
and dated the 1st of a coming month, which reads like advance issuance. So they are
flagged and kept. If they are actually data entry errors they should be rejected,
and that is a one line config change.

**The 94 `expired_before_issued` rows are legitimate or unknown**, not corrupt, so
they are flagged rather than rejected. Backdated renewals would explain them. This
is the one I would most want a business answer on.

**Business names should not be recased.** I keep the registry's casing and derive
a separate `business_name_key` for joins. If the consumer would rather have pretty
display names, that is a different choice and it belongs in a view, not here.

**A daily schedule is enough.** The publisher refreshes the current folder year
daily and I have not seen intra day updates, so a single daily run should not miss
anything. It does mean intra day status changes are invisible.

**The dataset only holds 2024 onwards.** The publisher says the categories were
streamlined in May 2024 and the history lives in separate datasets, and the only
folder years present are 24, 25 and 26. The year filter guards against older rows
appearing anyway, but those historical datasets are not ingested here.

## Schedule

Daily. Their current year extract landed around 07:08 UTC the day I built this,
so about 12:00 UTC leaves room for a late publish and we still get same day data.

Stage 2 transforms every year whenever stage 1 downloaded anything, for the
overlap reason above. About fifteen seconds for 205k rows.
