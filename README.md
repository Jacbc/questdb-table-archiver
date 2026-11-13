# questdb-table-archiver

Chunked Parquet export for [QuestDB](https://questdb.com/), built to survive
the 5-minute HTTP timeout that breaks single-stream exports of large tables.
For a whole-database backup you can restore from (schema, views, manifests,
incremental runs, verified restore), see
[Full backup and restore](#full-backup-and-restore-qdb-backuppy).

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
QDB=http://questdb.example:9000 ./qdb-export.sh --table ais 2024 2025 2026
```

Files are written to `<prefix>_parquet/<prefix>_<period>.parquet`, e.g.
`ais_parquet/ais_2026-04.parquet`. The prefix defaults to the table name;
`--prefix` keeps the old file names when a table is renamed:

```bash
# Table renamed from ais to ais_v2: keep writing ais_parquet/ais_YYYY-MM.parquet
./qdb-export.sh --table ais_v2 --prefix ais 2026
```

Export base tables, not views over them: a view adds a layer per query
and, if it filters or joins, may not pass the time range down to the
table's partitions.

## CLI reference

| Flag | Env | Default | Description |
|---|---|---|---|
| `-t, --table NAME` | `QDB_TABLE` | *(required)* | Table to export |
| `-c, --ts-col NAME` | `QDB_TS_COL` | auto-detect (see below) | Timestamp column name |
| `-b, --by GRANULARITY` | `QDB_BY` | `month` | Chunk size: `year` / `month` / `week` / `day` |
| `-p, --prefix NAME` | `QDB_PREFIX` | `<table>` | File name prefix |
| `-o, --out DIR` | `OUT_DIR` | `<prefix>_parquet` | Output directory |
| `-u, --url URL` | `QDB` | `http://localhost:9000` | QuestDB base URL |
| `--timeout SECS` | `TIMEOUT` | `280` | Per-chunk curl timeout |
| `--settle-days N` | `SETTLE_DAYS` | `1` | Days after a period ends before its file counts as final |

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

- **Resumable.** A file is skipped on rerun when it is complete (`PAR1`
  trailer) **and** was written at least `--settle-days` after its period
  ended. So interrupted runs pick up where they left off, and a period
  exported while still open (e.g. the current month) is re-exported on
  the next run and shown as `updated`, until a run after it has ended.
  This uses the file's modification time: copy archives with `rsync -a`
  or `cp -p`, or a copy made later looks final.
- **Never leaves a half-written file.** Each chunk downloads to
  `<file>.part` and replaces the target only after the `PAR1` check, so a
  failed re-export keeps the previous good file. Files that fail the
  check are saved as `.broken`.
- **Empty periods are deleted.** If a period falls outside the data
  range, the response is a tiny schema-only Parquet; the script
  confirms with a `count()` query and removes the file rather than
  cluttering the archive. If that `count()` fails, the chunk is reported
  as failed instead of empty.
- **Per-chunk timeout (default 280 s).** A few seconds shy of QuestDB's
  ~5 minute server-side limit, so failures bail fast.
- **Failed chunks leave a `.err` file** containing curl's stderr next to
  the target path, so you can see what went wrong without rerunning.
  It is removed when the chunk later succeeds or turns out empty.

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
# Merge everything in ais_parquet/ -> ais.parquet (zstd)
./qdb-merge.sh ais_parquet/

# Merge only 2023 chunks -> ais_2023.parquet
./qdb-merge.sh -p 2023 ais_parquet/

# Merge only April 2023 -> ais_2023-04.parquet
./qdb-merge.sh -p 2023-04 ais_parquet/

# Explicit output path
./qdb-merge.sh -p 2023 ais_parquet/ ais_archive_2023.parquet

# Different codec
./qdb-merge.sh -c snappy ais_parquet/ ais.parquet

# Sanity-check row counts after merge
./qdb-merge.sh --verify ais_parquet/
```

The default output filename is derived from the input directory: a
trailing `_parquet` is stripped, and the `--period` (if any) is appended.
So `ais_parquet/` produces `ais.parquet`, and `ais_parquet/ -p 2023`
produces `ais_2023.parquet`.

The `--period` filter matches any chunk filename containing
`_<period>` — `-p 2023` picks up `ais_2023.parquet`,
`ais_2023-01.parquet`, `ais_2023-04-15.parquet`, etc. So you can roll
daily chunks up into yearly archives without re-exporting.

Requires the [DuckDB CLI](https://duckdb.org/docs/installation/)
(`brew install duckdb` on macOS). Chunk filenames sort chronologically by
construction (`<table>_2023-01.parquet < <table>_2023-02.parquet < ...`),
so the merged file preserves time order without an explicit `ORDER BY`.

## Verifying a merge before deleting the chunks

After running `qdb-merge.sh` you may want to delete the granular chunk
files to reclaim disk. Before doing that, run `qdb-check.sh` to confirm
the merged file faithfully represents every row in the chunks. No
QuestDB connection is needed — it's a purely local comparison via DuckDB.

```bash
# Compare a merged file against its source dir
./qdb-check.sh ais_2023.parquet ais_parquet/ -p 2023

# Or pass an explicit glob (must be quoted)
./qdb-check.sh ais_2023.parquet "ais_parquet/*_2023*.parquet"

# Also diff column names
./qdb-check.sh --schema ais.parquet ais_parquet/
```

The script verifies:

1. **Row count** — `count(*)` of merged file == `count(*)` of chunk glob
2. **Time range** — `min/max(<ts>)` matches between merged and chunks
3. **Schema** (with `--schema`) — column names match

Exits 0 on full match, 1 on any mismatch. So you can guard a cleanup
step:

```bash
./qdb-check.sh ais_2023.parquet ais_parquet/ -p 2023 \
  && rm ais_parquet/*_2023*.parquet
```

Example output:

```
Comparing:
  merged: ais_2023.parquet  (17.9 GB)
  chunks: 9 file(s) from 'ais_parquet/*_2023*.parquet'  (29.7 GB)

Counting rows...
  merged: 842301557 rows
  chunks: 842301557 rows
  OK  row counts match

Comparing time range...
  merged: 2023-01-15T08:12:03.000000Z  ..  2023-12-31T23:59:58.000000Z
  chunks: 2023-01-15T08:12:03.000000Z  ..  2023-12-31T23:59:58.000000Z
  OK  ranges match

PASS  merged file faithfully represents the source chunks.
      Safe to delete the chunk files if you no longer need them.
```

## Full backup and restore (`qdb-backup.py`)

`qdb-export.sh` copies one table. `qdb-backup.py` keeps a **backup of a whole
database that you can rebuild a working QuestDB from**: every table (data or
schema only), the views, the exact table definitions, and a manifest per table
that says what each file holds and how it was checked. It writes the same file
names as `qdb-export.sh`, so an existing archive is taken over in place. From
then on, write that directory with `qdb-backup.py` only: a file that
`qdb-export.sh` replaces behind the manifest's back no longer matches it.
`verify` reports that; `run` notices only a changed file size (and then
exports the month again).

Requires Python 3.8+ (standard library only) and the
[DuckDB CLI](https://duckdb.org/docs/installation/), which checks every file
against the source before the backup trusts it. `run --no-duckdb` works
without it, explicitly: files are then not checked, and existing files are
adopted by their file time until a later run with DuckDB checks them.
`merge-year` and `verify --deep` always need it.

```bash
# back up what changed (run it by hand, or from cron on the backup machine)
python3 qdb-backup.py run --config backup.json --root /backups/qdb --url http://questdb:9000

# what would it do?
python3 qdb-backup.py run --config backup.json --root /backups/qdb --url http://questdb:9000 --dry-run

# after the year has ended: merge 2026's monthly files into <prefix>_2026.parquet (verified per day)
python3 qdb-backup.py merge-year --root /backups/qdb --year 2026 [--delete-months]

# check the files against the manifests, without QuestDB
python3 qdb-backup.py verify --root /backups/qdb [--deep]

# rebuild: an empty QuestDB of the same version, with the backup as its SQL copy root
docker run -d --name qdb-restore -p 9000:9000 \
  -v /backups/qdb:/backup:ro -v qdb-restore-data:/var/lib/questdb \
  -e QDB_CAIRO_SQL_COPY_ROOT=/backup questdb/questdb:<version in BACKUP.json>
python3 qdb-backup.py restore --backup /backups/qdb --url http://localhost:9000
```

### Config

JSON, see [`examples/backup.example.json`](examples/backup.example.json). One
entry per table:

| Key | Default | Meaning |
|---|---|---|
| `table` | | table name |
| `mode` | `chunked` | `chunked`: one Parquet file per month (needs a designated timestamp); `full`: the whole table every run (small tables, up to `--full-max-rows`); `schema`: DDL only; `skip`: not backed up |
| `prefix` | table name | file name prefix (keeps old names after a rename) |
| `dir` | `.` | directory under the backup root |
| `settle_days` | `1` | a month counts as final once it ended this many days ago |
| `start` | | `YYYY-MM`: months before it are kept elsewhere and never exported (an existing file of theirs is still adopted) |
| `timeout` | `280` | seconds per request; every month is one `/exp` request, so keep it below the server's statement timeout |

Every table in QuestDB has to be listed: a table the config does not mention
makes `run` exit with code 3 after it has done the listed ones (`"unlisted":
"warn"` turns that off). Views are captured automatically; a view that uses a
table the config does not back up is left out (and reported).

### Layout

```
ROOT/
  BACKUP.json                     source build, provenance (--note KEY=VALUE), config, per-table summary
  SHA256SUMS                      sha256sum -c compatible; data files from the manifests, schema files fresh
  schema/ddl/<t>.sql              generated, replayable DDL (CREATE TABLE ...; ALTER TABLE ... DEDUP ENABLE ...)
  schema/ddl/<t>.show.sql         SHOW CREATE TABLE, for reference
  schema/views/NN_<v>.sql         views in dependency order
  schema/tables.json, columns/, objects.json, create_database.sql, build.txt
  <dir>/<prefix>_parquet/<prefix>_YYYY-MM.parquet   chunked tables, one file per month
  <dir>/<prefix>_YYYY.parquet                       a merged year
  <dir>/<prefix>.parquet                            full tables
  <dir>/<prefix>.manifest.json                      per table: one entry per month and year
```

`SHOW CREATE TABLE` alone does not round-trip (no `WAL` keyword, no symbol
`CAPACITY`, no `WITH` parameters, and a TTL set with `ALTER TABLE` may be
missing), so the DDL is generated from `tables()` and `table_columns()`. A
restore checks the result against a whitelist of those fields (designated
timestamp, partitioning, WAL, DEDUP keys, TTL, `maxUncommittedRows`,
`o3MaxLag`; per column: name, type, symbol capacity and cache, index,
designated, upsert key).

### What a run does

For each chunked table, one `table_partitions()` query (no table scan) gives
rows, time range and the WAL sequencer txn of every partition, grouped by the
day and month they cover (never by partition name). Per month:

| Situation | Action |
|---|---|
| month not settled yet | export again (status `open`) |
| no manifest entry, a file is there | adopt it: its row count and time range must equal the source metadata (with `--no-duckdb`: the file was written after the month settled, checked properly on the next run with DuckDB) |
| no manifest entry, no file, month settled | export it (`new`); more than `--max-new` (default 3) such months in a table stop the run before any download: a wrong or empty backup root, or files not copied in |
| metadata equal to the manifest | skip: nothing is read from the table |
| metadata changed | fingerprint only the changed days; equal content (e.g. identical rows rewritten) updates the manifest without a download, otherwise the month is exported again |
| written to during the export | the file is kept but marked unstable and exported again next run |
| a settled month lost more than 10% of its rows | refused (a reload in progress?) unless `--accept-shrink` |
| month is only in a merged year file and changed | exported as a monthly file again and listed as `stale_months` of that year; restore uses the monthly file |
| month has no rows in the source any more | kept in the backup (`source_gone`) |

Each settled export is checked: the per-day fingerprint of the table (QuestDB,
one query per day) must equal the file's (DuckDB). Downloads go to `.part`
and replace the file only after the check; a short or truncated body (no
`PAR1` trailer, `Content-Length` mismatch) is a failure. The manifest is
written after every month, so an interrupted run loses at most one month.
`--deep-check` fingerprints settled months against the source again (a few
per run, `--deep-limit`); that catches what the metadata cannot see. A lock
file stops two runs on one backup, the free disk must be at least twice the
largest file, and the clock of the backup machine must agree with QuestDB's
`now()` within 5 minutes (the settle rule reads it).

### Fingerprint

Per day, with `P = 1000000007`: `count(*)`, `min`/`max` of the designated
timestamp (microseconds), and per column: `count(col)`, plus
`sum(cast(col AS LONG))` (BYTE/SHORT/INT), `sum(col % P)` (LONG),
`sum(us(col) % P)` (TIMESTAMP/DATE), `sum(floor(col * 1e6) % P)` for
`|col| < 1e12` (FLOAT/DOUBLE), the count of true values (BOOLEAN), and the
summed length of non-null values (STRING/VARCHAR/SYMBOL; QuestDB's
`length(NULL)` is -1, hence a `CASE`). UUID, IPv4, GEOHASH, LONG256 and BINARY
get a non-null count only; CHAR and arrays are not covered. The same numbers
come from QuestDB (a table, or `read_parquet`) and from DuckDB (a file); a
month is the sum of its days. String lengths are counted in UTF-8 bytes on
both sides (`length_bytes` in QuestDB, `strlen` in DuckDB), so non-ASCII text
compares exactly. DuckDB sessions always run with `SET TimeZone='UTC'` and
group by `epoch_us`.

### Types

Files written by QuestDB read back exactly through `read_parquet()`, except
SYMBOL, which comes back as VARCHAR (restore inserts it into the SYMBOL
column). A file **rewritten by DuckDB** (a merged year) changes CHAR, IPv4,
GEOHASH, LONG256 (lost), TIMESTAMP_NS (cut to µs), UUID, BINARY and arrays,
so tables with those types cannot use `merge-year`; they keep monthly files. STRING becomes VARCHAR and DATE
becomes TIMESTAMP, which restore casts back.

### Restore

`restore` needs only the backup directory and an empty QuestDB that reads it as
its copy root:

1. checks the version, that the copy root is this backup, and that the target
   has none of the tables (unless `--resume`);
2. creates each table **without DEDUP**, so rows with equal keys in the
   backup are all kept; DEDUP is enabled after the load;
3. loads full tables, then chunked tables day by day
   (`INSERT INTO t (cols) SELECT cast(...) FROM read_parquet(file) WHERE ts IN 'day'`),
   by column name: a column added later is NULL in older months;
4. checks each month: the table's per-day fingerprint must equal the file's
   (and the one recorded at backup time when the export was stable); a day that
   does not match is dropped and loaded again on `--resume`;
5. creates the views, then checks the schema whitelist, row counts and that
   every view can be queried. Exit code 0 only when everything matches.

Options: `--tables a,b` (no views then), `--without DIR` (tables stored under
DIR are created but get no data, for a copy shared without that directory),
`--from/--to YYYY-MM`, `--resume`, `--no-sha` (skip the sha256 check of each
file before it is loaded).

### Year end

`merge-year` needs every month of the year as a settled, stable monthly file
with the same columns (re-export missing ones with `run --refetch YYYY-MM`).
DuckDB writes `<prefix>_YYYY.parquet` (`preserve_insertion_order`, ZSTD,
spilling to `ROOT/.duckdb-tmp`); its per-day fingerprint must equal the
monthly files'. Only then is it renamed into place, and only with
`--delete-months` are the monthly files removed.

## Limitations / not yet implemented

- No compression flag yet — uses QuestDB's server-side default. To
  change globally, set
  [`cairo.parquet.export.compression.codec`](https://questdb.com/docs/configuration/#parquet)
  on the QuestDB server.
- `qdb-backup.py` does not back up materialized views: list them as `skip`
  and recreate them by hand after a restore. A table whose single day does not
  fit in one request cannot be backed up with it.

## License

MIT — see [LICENSE](./LICENSE).
