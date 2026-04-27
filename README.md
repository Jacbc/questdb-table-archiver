# questdb-table-archiver

Chunked Parquet export for [QuestDB](https://questdb.com/), built to survive
the 5-minute HTTP timeout that breaks single-stream exports of large tables.

## The problem

QuestDB offers two ways to export a table to Parquet:

1. **REST `/exp?fmt=parquet`** ([docs](https://questdb.com/docs/query/export-parquet/)) —
   synchronous HTTP streaming. Easy to use, files arrive on the client.
2. **`COPY ... TO ... WITH FORMAT PARQUET`** ([docs](https://questdb.com/docs/reference/sql/copy/)) —
   asynchronous, server-side. Files land on the QuestDB host's filesystem.

For self-hosted Docker deployments (e.g. via [Coolify](https://coolify.io/)),
method 2 lands the files inside the container at `/root/.questdb/export/`,
which by default is **outside** the persistent volume mount (only
`/root/.questdb/db/` is mounted). Pulling them out requires `docker cp`,
host shell access, or a config change to add an export volume.

Method 1 dodges the file-transfer problem because results stream straight to
the HTTP client. But QuestDB's HTTP server has a default ~5 minute idle
connection timeout. For tables larger than ~20 GB of Parquet output, the
server-side encoding/compression of row groups can pause the byte stream
long enough to trip that timeout. The transfer dies with:

```
curl: (18) transfer closed with outstanding read data remaining
```

…and the partial file is **unreadable**, because Parquet stores its
metadata in a footer at end-of-file (the `PAR1` magic bytes) that never
gets written.

CSV exports survive longer because rows stream continuously without
buffering, but they're 10–15× larger on disk and need a client-side
conversion step (e.g. via [DuckDB](https://duckdb.org/)).

## The fix

Issue many smaller queries, each filtering to a time slice that fits
comfortably under the timeout:

```sql
SELECT * FROM ais WHERE time IN '2026-04';    -- one month
SELECT * FROM ais WHERE time IN '2026-W17';   -- one ISO week
SELECT * FROM ais WHERE time IN '2026-04-27'; -- one day
```

QuestDB's `IN` operator on a timestamp column accepts a partial-string
prefix that expands to a range and uses partition metadata to prune
efficiently — see [interval scan syntax](https://questdb.com/docs/concept/interval-scan/).

Each query produces a complete, valid Parquet file. The directory of
files reads as a single dataset under DuckDB / Polars / pandas.

## Install

Drop the script anywhere on your `$PATH`:

```bash
curl -O https://raw.githubusercontent.com/<you>/questdb-table-archiver/main/qdb-export.sh
chmod +x qdb-export.sh
```

Requires `bash` 4+, `curl`, and `python3` (used only for date math and JSON
parsing — both are on every macOS and modern Linux box by default).

## Quickstart

```bash
# Single month of the `ais` table
./qdb-export.sh --table ais 2026-04

# Whole year, expanded into 12 monthly files
./qdb-export.sh --table ais 2025

# Auto-detect the ts column and the data range, iterate by month
./qdb-export.sh --table ais

# Daily granularity for a high-volume table
./qdb-export.sh --table trades --by day 2026-04

# Point at a remote QuestDB
QDB=http://192.168.4.6:9000 ./qdb-export.sh --table ais 2024 2025 2026
```

Files are written to `<table>_parquet/<table>_<period>.parquet`, e.g.
`ais_parquet/ais_2026-04.parquet`.

## CLI reference

| Flag | Env | Default | Description |
|---|---|---|---|
| `-t, --table NAME` | `QDB_TABLE` | *(required)* | Table to export |
| `-c, --ts-col NAME` | `QDB_TS_COL` | auto-detect (see below) | Timestamp column name |
| `-b, --by GRANULARITY` | `QDB_BY` | `month` | Chunk size: `year` / `month` / `week` / `day` |
| `-o, --out DIR` | `OUT_DIR` | `<table>_parquet` | Output directory |
| `-u, --url URL` | `QDB` | `http://localhost:9000` | QuestDB base URL |
| `--timeout SECS` | `TIMEOUT` | `280` | Per-chunk curl timeout |

### Period formats

Pass any combination as positional args:

| Format | Meaning |
|---|---|
| `YYYY` | Full year — expanded into chunks at `--by` granularity |
| `YYYY-MM` | Single month |
| `YYYY-Www` | ISO week |
| `YYYY-MM-DD` | Single day |

If no periods are given, the script runs `SELECT min(<ts>), max(<ts>)` on
the table and iterates the entire range at `--by` granularity.

### Timestamp column detection

If you don't pass `--ts-col`, the script picks one in this order:

1. The table's **designated timestamp**, if one is set
   (`SELECT designatedTimestamp FROM tables()`).
2. The first column with type `TIMESTAMP` whose name matches a common
   convention: `ts`, `time`, `timestamp`, `event_time`, `created_at`,
   `dt`, `date` (case-insensitive).
3. Failing that, the first `TIMESTAMP`-typed column on the table.

If none of those work — or you want a different column than the one
auto-detection lands on — pass `--ts-col` explicitly.

## Behaviors that matter

- **Resumable.** Existing complete files (verified by `PAR1` trailer) are
  skipped on rerun, so interrupted runs pick up where they left off.
- **Self-validating.** Every fetched file is checked for the `PAR1`
  trailer before being kept. Files that fail the check are renamed
  `.broken` and re-fetched on the next run.
- **Empty periods are deleted.** If a period falls outside the data
  range, the response is a tiny schema-only Parquet; the script
  confirms with a `count()` query and removes the file rather than
  cluttering the archive.
- **Per-chunk timeout (default 280 s).** A few seconds shy of QuestDB's
  ~5 minute server-side limit, so failures bail fast.
- **Failed chunks leave a `.err` file** containing curl's stderr next to
  the target path, so you can see what went wrong without rerunning.

## Reading the output

```python
# DuckDB
duckdb.sql("SELECT count(*) FROM read_parquet('ais_parquet/*.parquet')")

# Polars
pl.scan_parquet('ais_parquet/*.parquet').collect()

# pandas + pyarrow
pd.read_parquet('ais_parquet/')
```

## Merging into a single file

If a downstream tool prefers one Parquet file over a directory, use the
companion `qdb-merge.sh` script. It streams all chunks through DuckDB's
`COPY` so memory usage stays bounded regardless of total size.

```bash
# Default: writes ais_parquet.parquet next to the input dir, zstd-compressed
./qdb-merge.sh ais_parquet/

# Explicit output path
./qdb-merge.sh ais_parquet/ ais_full.parquet

# Different compression
./qdb-merge.sh -c snappy ais_parquet/ ais_full.parquet

# Verify row counts match between inputs and output
./qdb-merge.sh --verify ais_parquet/
```

Requires the [DuckDB CLI](https://duckdb.org/docs/installation/)
(`brew install duckdb` on macOS). Chunk filenames sort chronologically by
construction (`<table>_2023-01.parquet < <table>_2023-02.parquet < ...`),
so the merged file preserves time order without an explicit `ORDER BY`.

## Limitations / not yet implemented

- No compression flag yet — uses QuestDB's server-side default. To
  change globally, set
  [`cairo.parquet.export.compression.codec`](https://questdb.com/docs/configuration/#parquet)
  on the QuestDB server.
- No `manifest.json` summarizing what was exported.
- No `--verify` mode that round-trips each file through DuckDB to
  confirm row-level validity.

## License

MIT — see [LICENSE](./LICENSE).
