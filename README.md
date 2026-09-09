# Pz.Connector.BigQuery

Google BigQuery source and sink for [PipelineZ](https://pipelinez.dev) (`pz`), served out of
process. A table reads through the **Storage Read API** as Arrow, partitioned across streams; a
sink stages rows as a load job and lands them into the target with a single append DML, a
`WRITE_TRUNCATE` replace, or a `MERGE`.

## Installation

```yaml
# project.yml
connectors:
  - package: Pz.Connector.BigQuery
    version: 0.1.0
```

`pz restore` installs the Native AOT binary for your platform (linux-x64, linux-arm64, osx-arm64,
win-x64) and `pz run` spawns it. Needs pz 0.6.0 or newer.

| RID | status |
|---|---|
| `linux-x64` | the platform every test and the packaging proof run on |
| `linux-arm64`, `osx-arm64`, `win-x64` | shipped, never exercised by this connector's own CI |

## Capabilities

| Capability | Meaning |
|---|---|
| `ColumnPruning` | a read pushes `selected_fields` into the Storage Read API session |
| `PredicatePushdown` | `ReadHints.PredicateSql` is pushed into `row_restriction` verbatim |
| `BoundedWindow` | a bounded incremental window (`initial`/`max_window`/`until`) is supported |
| `InclusiveWatermarkBound` | the watermark's lower bound can be inclusive as well as exclusive |
| `PartitionedRead` | a read fans out across the streams the Storage Read API session grants |
| `Merge` | a sink output can declare `strategy: merge` with `keys:` |
| `ReplaceWrites` | a sink output can declare `strategy: replace` |
| `Transactional` | exactly one job touches the write target; a crash before it never leaves a partial write, and `AbortAsync` discards everything staged |

There is no native scan or native copy: BigQuery has no DuckDB extension, so every read and write
goes through the Arrow `RecordBatch` path.

## Connection

```yaml
# connections.yml
bq:
  connector: bigquery
  project: my-gcp-project            # required: billing project and default project for 2-part entity names
  auth: service_account              # required: service_account | adc | none
  key_file: secrets/bq-sa.json       # service_account: exactly one of key_file / key_json
  key_json: ${BQ_KEY_JSON}
  location: EU                       # optional: job location; omitted = BigQuery infers from the dataset
  staging_dataset: pz_staging        # optional: home of query-mode result tables and sink staging tables
  endpoint: http://localhost:9050    # optional: REST base override (emulator / proxy)
  storage_endpoint: localhost:9060   # optional: Storage API gRPC endpoint override (emulator / proxy)
```

- `project` must match `[a-z0-9.:-]+` -- it is spliced into REST paths and generated SQL.
- `auth: service_account` takes exactly one of `key_file` (a project-relative path) or `key_json`
  (the unmodified service-account key JSON); `auth: adc` takes neither and resolves through
  Application Default Credentials; `auth: none` sends no `Authorization` header at all and is only
  accepted when `endpoint` is also set -- meaningful against an emulator or an authenticating
  proxy, never against Google.
- `staging_dataset` is required for any `query:` read (BigQuery cannot stream an arbitrary query's
  results directly) and is where every sink's staging tables land when set; otherwise a sink stages
  into the target's own dataset.
- `endpoint`/`storage_endpoint` point both clients at an emulator; a plaintext gRPC channel is used
  only when `endpoint` is `http://`.

`key_json`, every minted access token, and any `Authorization: Bearer` header echoed back in an
error are redacted from every message.

`pz connector check` (`CheckConnectionAsync`) lists datasets (`GET
/bigquery/v2/projects/{project}/datasets?maxResults=1`) and reports how many are visible, or
`authenticated` when the listing is empty.

## Naming an entity

An entity name is `dataset.table` or `project.dataset.table`. A two-part name uses the
connection's `project`. Backticks are not accepted -- the name is the object name, not a SQL
fragment.

```yaml
  entities:
    orders:
      read:
        entity: sales.orders           # optional; defaults to the pz entity name itself
        streams: 4                     # optional; 1..64, default 4
    recent_orders:
      read:
        query: select * from `sales.orders` where region = 'EU'   # query mode, below
    orders_out:
      write:
        entity: marts.orders_daily     # optional; defaults to the pz entity name
```

## Reading data

### Table mode (default)

`entity:` names a table directly. Column pruning (`hints.Columns`) becomes `selected_fields`;
predicate pushdown and the incremental watermark become an AND of parenthesized
`row_restriction` terms, e.g. `(region = 'EU') AND (updated_at > TIMESTAMP '...')`. The cursor
literal is typed from the table's own schema (a numeric column becomes a bare number, a date
becomes `DATE '...'`, a string is quoted and escaped, and so on) -- an unsupported cursor type or a
cursor column absent from the schema is refused before any request is sent. A predicate containing
a double quote is never pushed down (GoogleSQL reads `"..."` as a string literal, not the quoted
identifier DuckDB means it as) and is instead left for the engine to filter locally.

`streams:` (1-64, default 4) is the `max_stream_count` asked of the Storage Read API session; the
service may grant fewer, and an empty table grants zero, which reads as zero rows.

### Query mode (`query:`)

`query:` runs arbitrary GoogleSQL instead of naming a table directly. It requires
`staging_dataset` on the connection: the query is materialized once into a temporary table
(`WRITE_TRUNCATE`, `CREATE_IF_NEEDED`) before the Storage Read API can stream it back, and pruning
and predicate pushdown are still applied by the read session against that materialized table --
never by rewriting the query text. `entity:` and `query:` cannot both be set on the same read.

The materialized table is dropped when the read session closes; a killed process still cannot leak
it permanently, since its `expirationTime` is set to now + 6 hours immediately after materialization.

### Type mapping (Storage Read API to Arrow)

| GoogleSQL type | Arrow column type |
|---|---|
| `BOOL` | boolean |
| `INT64` | 64-bit integer |
| `FLOAT64` | double |
| `NUMERIC(p,s)` | decimal128 (default precision 38, scale 9) |
| `BIGNUMERIC(p,s)` | decimal256 (default precision 76, scale 38) |
| `STRING`, `GEOGRAPHY`, `JSON` | utf8 |
| `BYTES` | binary |
| `DATE` | date32 |
| `DATETIME` | timestamp, microsecond, no time zone |
| `TIMESTAMP` | timestamp, microsecond, UTC |
| `TIME` | time64, microsecond |
| `ARRAY` | list |
| `STRUCT`, `RANGE` | struct |

**BIGNUMERIC limitation**: DuckDB's Arrow ingest has no decimal256 type, so a `BIGNUMERIC` column
fails ingest with DuckDB's own error. Work around it with `query:` and an explicit cast --
`cast(col as string)` or `cast(col as numeric)` -- to bring the value in as a type DuckDB accepts.

**Views are refused.** The Storage Read API cannot read a view or materialized view; reading one
directly fails naming the view, with the hint to use `query: select * from <view>` instead, which
materializes it like any other query.

## Writing data

Every write stages rows to a local NDJSON spool, then commits with exactly one job that touches
the target -- a crash before that job runs leaves the target untouched, and a crash after it
completes is safe to retry (append is at-least-once, replace and merge are idempotent).

1. `BeginWriteAsync` validates the mode (`append`/`replace`/`merge`), the merge keys (present in
   the schema, and never the reserved column `_pz_seq`), and maps the Arrow schema to a BigQuery
   schema -- entirely offline.
2. Every batch is appended to the spool as NDJSON; a file rolls once it exceeds 64 MiB.
3. On commit, the spool is loaded into a staging table (`tables.insert` plus one load job per
   spool file), the target's existing schema is checked (or the target is created if it doesn't
   exist), and exactly one statement moves staged rows into the target:
   - **`append`**: `insert into <target> (...) select ... from <staging>` when the target already
     exists (so extra target columns and the target's own types are respected), or a
     query-destination job with `writeDisposition: WRITE_APPEND` when it doesn't.
   - **`replace`**: a query-destination job with `writeDisposition: WRITE_TRUNCATE` -- atomic in
     BigQuery, and it replaces the target's schema with the write's own (a consequence worth
     knowing if the target carries extra columns).
   - **`merge`**: staged rows are deduplicated on the merge keys (last row within the session wins;
     null keys match null keys). When the target already exists, `MERGE ... WHEN MATCHED THEN
     UPDATE ... WHEN NOT MATCHED THEN INSERT ...` runs against it. `MERGE`'s own syntax requires an
     existing target, so the first commit into a target that doesn't exist yet instead creates it
     with a query-destination job over that same deduplicated `SELECT` -- one query job, no `MERGE`
     statement; every commit after that one runs the real `MERGE`.
4. The staging table is dropped (best effort) and the spool directory is deleted.

An empty write still runs its target job: an empty append inserts nothing, an empty replace
truncates the target, an empty merge is a no-op, and a missing target is still created either way.

### Type mapping (Arrow to BigQuery)

| Arrow type | BigQuery type | NDJSON encoding |
|---|---|---|
| int8/16/32/64, uint8/16/32 | `INT64` | number |
| uint64 | `NUMERIC` (INT64 cannot hold 2^64) | digit string |
| float/double | `FLOAT64` | number; NaN/±Infinity as the strings `"NaN"`/`"Infinity"`/`"-Infinity"` |
| decimal128(p≤38, s≤9) | `NUMERIC(p,s)` | digit string |
| decimal128/256 otherwise (p≤76, s≤38) | `BIGNUMERIC(p,s)` | digit string |
| utf8/large utf8 | `STRING` | string |
| binary/large binary/fixed-size binary | `BYTES` | base64 string |
| boolean | `BOOL` | boolean |
| date32/date64 | `DATE` | `yyyy-MM-dd` |
| timestamp with time zone | `TIMESTAMP` | `yyyy-MM-ddTHH:mm:ss.ffffffZ` (UTC) |
| timestamp without time zone | `DATETIME` | `yyyy-MM-ddTHH:mm:ss.ffffff` |
| time32/time64 | `TIME` | `HH:mm:ss.ffffff` |
| list/struct/map/union/dictionary/interval/duration/null | refused (`PZBQ0303`) | -- |

On the wire, the connector emits BigQuery's legacy type aliases (`INTEGER`, `FLOAT`, `BOOLEAN`)
rather than the GoogleSQL names in the table above -- the emulator used in this connector's own
tests accepts only the legacy spellings, while real BigQuery accepts both spellings everywhere and
treats them as the same type. Schema comparison against an existing target (`fail_on_change`,
below) normalizes both spellings, so a target created through the console with
`INT64`/`FLOAT64`/`BOOL` columns is not a mismatch.

Every mapped column is `NULLABLE`; `MaxTextLengths` is ignored (`STRING` is unbounded).

### Schema policy

Only `fail_on_change` (the default) is supported: when the target already exists, every write
column must be present with a compatible type and mode, or the write is refused listing every
mismatch. `schema_policy: evolve` is refused outright -- align the target by hand, or drop it and
let the sink recreate it. An extra column the write doesn't mention is left alone (`NULL` on
insert, kept as-is on merge update).

### Cost

A load job (staging the spool) is free of BigQuery query cost. The statement that finally touches
the target, and a `query:` read's materialization, are billed as ordinary queries against the
bytes they scan. A staging table that a crash prevents from being cleaned up still expires on its
own after 6 hours.

### IAM roles

| To | Needs |
|---|---|
| read | `roles/bigquery.dataViewer` + `roles/bigquery.readSessionUser` |
| write | `roles/bigquery.dataEditor` + `roles/bigquery.jobUser` |

## Errors

Every failure is `bigquery: PZBQ####: <redacted text>`.

| Code | Meaning |
|---|---|
| `PZBQ0101` | `project` is missing or doesn't match `[a-z0-9.:-]+` |
| `PZBQ0102` | `auth` is missing or not one of `service_account`/`adc`/`none` |
| `PZBQ0103` | `service_account` auth needs exactly one of `key_file`/`key_json` |
| `PZBQ0104` | `auth: none` was set without `endpoint` |
| `PZBQ0105` | a `query:` read needs `staging_dataset` on the connection |
| `PZBQ0106` | Application Default Credentials could not be resolved |
| `PZBQ0107` | `key_file` could not be loaded from disk |
| `PZBQ0108` | `key_json` is not a valid service-account key |
| `PZBQ0109` | the connection config failed validation (aggregate) |
| `PZBQ0201` | an entity name is not `dataset.table` or `project.dataset.table` |
| `PZBQ0202` | the incremental cursor column is absent from the table's schema |
| `PZBQ0203` | the cursor column's type has no supported watermark literal form |
| `PZBQ0204` | the target is a view or materialized view; the Storage Read API cannot read it -- use `query:` |
| `PZBQ0205` | the table was not found |
| `PZBQ0206` | the Storage Read API denied the read (needs `bigquery.readsessions.create` + `bigquery.tables.getData`) |
| `PZBQ0207` | a dataset read option is unknown or invalid (e.g. `entity` and `query` both set, `streams` out of range) |
| `PZBQ0301` | a merge key column is missing from the write schema |
| `PZBQ0302` | the write schema uses the reserved column name `_pz_seq` |
| `PZBQ0303` | an Arrow column type has no BigQuery mapping |
| `PZBQ0304` | the existing target's schema doesn't match the write (`fail_on_change`) |
| `PZBQ0305` | `schema_policy: evolve` was requested; only `fail_on_change` is supported |
| `PZBQ0306` | an unsupported write mode reached the sink (only `append`/`replace`/`merge`) |
| `PZBQ0307` | a write output option is unknown or invalid |
| `PZBQ0401` | no response reached BigQuery (network/timeout) -- transient |
| `PZBQ0402` | BigQuery reported a transient service condition (`429`/`5xx`, `rateLimitExceeded`, `backendError`, `internalError`, `jobBackendError`, `jobInternalError`) -- transient |
| `PZBQ0403` | a hard quota was exceeded -- not transient, retrying inside the run cannot help |
| `PZBQ0404` | `401` -- check the service-account key or Application Default Credentials |
| `PZBQ0405` | `403`/`accessDenied` -- see the IAM roles above |
| `PZBQ0406` | `404`/`notFound` -- the named resource doesn't exist |
| `PZBQ0407` | `400`/`invalid`/`invalidQuery` -- BigQuery's own message, naming the SQL position |
| `PZBQ0408` | the Storage Read API's gRPC transport reported a transient status |
| `PZBQ0409` | any other remote failure -- not transient |

## Development

```bash
dotnet build Pz.Connector.BigQuery.slnx -c Release
dotnet test Pz.Connector.BigQuery.slnx -c Release --no-build      # Docker facts need docker; they SKIP without it
dotnet restore src/Pz.Connector.BigQuery -r linux-x64             # once, on a cold cache
dotnet publish src/Pz.Connector.BigQuery -c Release -r linux-x64 --no-restore
dotnet pack src/Pz.Connector.BigQuery -c Release -o packages      # nupkg with pz.connector.json
```

The `Docker`-tagged facts start `ghcr.io/goccy/bigquery-emulator:0.8.1` through Testcontainers
(`--project=test --dataset=e2e`) and SKIP without docker. `tests/e2e/` is the pz project CI runs
against a packed nupkg, against the same emulator started by hand:

```bash
docker run -d --name bq -p 9050:9050 -p 9060:9060 \
  ghcr.io/goccy/bigquery-emulator:0.8.1 --project=test --dataset=e2e
```

The `LiveBigQuery`-tagged facts run against a real project instead of the emulator, and SKIP
unless both environment variables are set:

- `PZ_BIGQUERY_PROJECT` -- a GCP project id billed for the facts' jobs
- `GOOGLE_APPLICATION_CREDENTIALS` -- a path to a service-account key with the write and read IAM
  roles above

Releases are tag-triggered (`v*`) and publish to nuget.org through trusted publishing.
