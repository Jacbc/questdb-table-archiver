#!/usr/bin/env bash
#
# qdb-check.sh — verify a merged Parquet file against its source chunks.
#
# Run this BEFORE deleting the granular chunked files to confirm the
# merge preserved every row. No QuestDB connection needed — purely a
# local comparison via DuckDB.
#
# Checks:
#   1. Row count: merged file == sum of chunks
#   2. Time range: min/max(<ts>) match
#   3. Schema (with --schema): column names match
#
# Exits 0 on full match, 1 on any mismatch. Suitable for use in scripts
# guarding a cleanup step:
#
#   ./qdb-check.sh ais_2023.parquet ais_parquet/ -p 2023 \
#     && rm ais_parquet/*_2023*.parquet
#
# Requires: bash 4+, duckdb CLI (brew install duckdb).

set -euo pipefail

PERIOD=""
TS_COL=""
SCHEMA=0

usage() {
  cat <<EOF
Usage: $(basename "$0") [options] <merged-file> <chunks-input>

Verify a merged Parquet file against its source chunked files.

Options:
  -p, --period PREFIX    When <chunks-input> is a directory, only compare
                         against files matching *_<PREFIX>*.parquet
                         (mirrors qdb-merge.sh -p).
  -c, --ts-col NAME      Timestamp column for range check (auto-detected
                         from the merged file's schema if omitted).
      --schema           Also compare column names between merged and chunks.
  -h, --help             Show this help.

Arguments:
  merged-file    The combined Parquet file produced by qdb-merge.sh.
  chunks-input   Either a directory (e.g. ais_parquet/) or a glob in
                 quotes (e.g. "ais_parquet/*_2023*.parquet").

Examples:
  $(basename "$0") ais.parquet ais_parquet/
  $(basename "$0") ais_2023.parquet ais_parquet/ -p 2023
  $(basename "$0") ais_2023.parquet "ais_parquet/*_2023*.parquet"
  $(basename "$0") --schema ais.parquet ais_parquet/

Use as a guard before cleanup:
  $(basename "$0") ais_2023.parquet ais_parquet/ -p 2023 \\
    && rm ais_parquet/*_2023*.parquet
EOF
}

POSITIONAL=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--period)  PERIOD="$2"; shift 2 ;;
    -c|--ts-col)  TS_COL="$2"; shift 2 ;;
    --schema)     SCHEMA=1;    shift ;;
    -h|--help)    usage; exit 0 ;;
    --)           shift; POSITIONAL+=("$@"); break ;;
    -*)           echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
    *)            POSITIONAL+=("$1"); shift ;;
  esac
done

if [[ ${#POSITIONAL[@]} -lt 2 ]]; then
  echo "Error: both <merged-file> and <chunks-input> are required" >&2
  usage >&2
  exit 2
fi

MERGED="${POSITIONAL[0]}"
CHUNKS_INPUT="${POSITIONAL[1]}"

if [[ ! -f "$MERGED" ]]; then
  echo "Error: merged file '$MERGED' not found" >&2
  exit 1
fi

if ! command -v duckdb >/dev/null 2>&1; then
  echo "Error: duckdb CLI not found. Install: brew install duckdb" >&2
  exit 1
fi

# ---------- helpers ---------------------------------------------------------

human_bytes() {
  awk -v b="$1" 'BEGIN {
    split("B KB MB GB TB PB", u, " ")
    i = 1
    while (b >= 1024 && i < 6) { b /= 1024; i++ }
    if (i == 1) printf "%d %s", b, u[i]
    else        printf "%.1f %s", b, u[i]
  }'
}

filesize() {
  stat -f%z "$1" 2>/dev/null || stat -c%s "$1" 2>/dev/null || echo 0
}

duck_scalar() {
  duckdb -noheader -list -c "$1" | tr -d ' \r'
}

# ---------- resolve the chunks input ---------------------------------------

if [[ -d "$CHUNKS_INPUT" ]]; then
  CHUNKS_DIR="${CHUNKS_INPUT%/}"
  if [[ -n "$PERIOD" ]]; then
    CHUNKS_GLOB="${CHUNKS_DIR}/*_${PERIOD}*.parquet"
  else
    CHUNKS_GLOB="${CHUNKS_DIR}/*.parquet"
  fi
else
  if [[ -n "$PERIOD" ]]; then
    echo "Warning: --period is ignored when <chunks-input> is a glob, not a directory" >&2
  fi
  CHUNKS_GLOB="$CHUNKS_INPUT"
fi

shopt -s nullglob
CHUNK_FILES=( $CHUNKS_GLOB )
shopt -u nullglob

if (( ${#CHUNK_FILES[@]} == 0 )); then
  echo "Error: no files match '${CHUNKS_GLOB}'" >&2
  exit 1
fi

CHUNK_BYTES=0
for f in "${CHUNK_FILES[@]}"; do
  CHUNK_BYTES=$((CHUNK_BYTES + $(filesize "$f")))
done
MERGED_BYTES=$(filesize "$MERGED")

# ---------- detect timestamp column ----------------------------------------

if [[ -z "$TS_COL" ]]; then
  TS_COL=$(duckdb -noheader -list -c "
WITH cols AS (
  SELECT column_name, data_type
  FROM (DESCRIBE SELECT * FROM read_parquet('${MERGED}'))
),
ts AS (
  SELECT column_name FROM cols WHERE upper(data_type) LIKE 'TIMESTAMP%'
),
preferred AS (
  SELECT column_name, list_position(['ts','time','timestamp','event_time','created_at','dt','date'], lower(column_name)) AS rank
  FROM ts
)
SELECT column_name FROM preferred ORDER BY (rank IS NULL), rank, column_name LIMIT 1;
" | tr -d ' \r')
  if [[ -z "$TS_COL" ]]; then
    echo "Warning: no TIMESTAMP column found in merged file; skipping range check" >&2
  fi
fi

# ---------- gather counts and ranges ---------------------------------------

echo "Comparing:"
echo "  merged: ${MERGED}  ($(human_bytes "$MERGED_BYTES"))"
echo "  chunks: ${#CHUNK_FILES[@]} file(s) from '${CHUNKS_GLOB}'  ($(human_bytes "$CHUNK_BYTES"))"
echo

echo "Counting rows..."
MERGED_ROWS=$(duck_scalar "SELECT count(*) FROM read_parquet('${MERGED}');")
CHUNK_ROWS=$(duck_scalar "SELECT count(*) FROM read_parquet('${CHUNKS_GLOB}');")

printf "  merged: %s rows\n" "$MERGED_ROWS"
printf "  chunks: %s rows\n" "$CHUNK_ROWS"

EXIT=0

if [[ "$MERGED_ROWS" == "$CHUNK_ROWS" ]]; then
  echo "  OK  row counts match"
else
  DIFF=$((CHUNK_ROWS - MERGED_ROWS))
  echo "  MISMATCH  diff: ${DIFF} (chunks - merged)"
  EXIT=1
fi

if [[ -n "$TS_COL" ]]; then
  echo
  echo "Comparing ${TS_COL} range..."
  MERGED_MIN=$(duck_scalar "SELECT min(${TS_COL}) FROM read_parquet('${MERGED}');")
  MERGED_MAX=$(duck_scalar "SELECT max(${TS_COL}) FROM read_parquet('${MERGED}');")
  CHUNK_MIN=$(duck_scalar "SELECT min(${TS_COL}) FROM read_parquet('${CHUNKS_GLOB}');")
  CHUNK_MAX=$(duck_scalar "SELECT max(${TS_COL}) FROM read_parquet('${CHUNKS_GLOB}');")

  printf "  merged: %s  ..  %s\n" "$MERGED_MIN" "$MERGED_MAX"
  printf "  chunks: %s  ..  %s\n" "$CHUNK_MIN" "$CHUNK_MAX"

  if [[ "$MERGED_MIN" == "$CHUNK_MIN" && "$MERGED_MAX" == "$CHUNK_MAX" ]]; then
    echo "  OK  ranges match"
  else
    echo "  MISMATCH  time ranges differ"
    EXIT=1
  fi
fi

if (( SCHEMA )); then
  echo
  echo "Comparing column schema..."
  MERGED_COLS=$(duckdb -noheader -list -c "
SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('${MERGED}'))
ORDER BY column_name;" | sed 's/^ *//;s/ *$//')
  CHUNK_COLS=$(duckdb -noheader -list -c "
SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('${CHUNKS_GLOB}'))
ORDER BY column_name;" | sed 's/^ *//;s/ *$//')

  if [[ "$MERGED_COLS" == "$CHUNK_COLS" ]]; then
    echo "  OK  column names match"
  else
    echo "  MISMATCH  column-name diff (merged vs chunks):"
    diff <(echo "$MERGED_COLS") <(echo "$CHUNK_COLS") | sed 's/^/    /'
    EXIT=1
  fi
fi

echo
if (( EXIT == 0 )); then
  echo "PASS  merged file faithfully represents the source chunks."
  echo "      Safe to delete the chunk files if you no longer need them."
else
  echo "FAIL  do NOT delete the chunk files until the mismatch is resolved."
fi

exit "$EXIT"
