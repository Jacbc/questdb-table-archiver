#!/usr/bin/env bash
#
# qdb-merge.sh — merge a directory of chunked Parquet files into one file.
#
# Companion to qdb-export.sh. Uses DuckDB's read_parquet + COPY to stream
# all input files into a single output Parquet without loading everything
# into memory. The chunk filenames produced by qdb-export.sh sort in
# chronological order lexicographically, so glob order is already correct.
#
# Optional --period filter narrows the merge to chunks for a single year,
# month, week, or day (e.g. --period 2023, --period 2023-04).
#
# Requires: bash 4+, duckdb CLI (brew install duckdb).

set -euo pipefail

CODEC="zstd"
VERIFY=0
ROW_GROUP_SIZE=""
PERIOD=""

usage() {
  cat <<EOF
Usage: $(basename "$0") [options] <input-dir> [output-file]

Merge chunked .parquet files in <input-dir> into a single Parquet file.

Options:
  -p, --period PREFIX       Only merge files whose name contains _<PREFIX>
                            (e.g. 2023, 2023-04, 2023-W17, 2023-04-15).
                            Matches both monthly and daily chunks for the
                            same span (e.g. -p 2023 picks up ais_2023-*).
  -c, --compression CODEC   zstd|snappy|gzip|lz4|uncompressed (default: zstd)
      --row-group-size N    Target row group size (default: DuckDB default)
      --verify              Compare row counts of input vs output after merge
  -h, --help                Show this help

Arguments:
  input-dir     Directory containing chunked .parquet files
  output-file   Output path. Default is derived from <input-dir> with the
                trailing "_parquet" stripped, plus the period if --period
                was given:
                  ais_parquet/                 -> ais.parquet
                  ais_parquet/ + -p 2023       -> ais_2023.parquet
                  ais_parquet/ + -p 2023-04    -> ais_2023-04.parquet

Examples:
  $(basename "$0") ais_parquet/                          # all files -> ais.parquet
  $(basename "$0") -p 2023 ais_parquet/                  # 2023 only -> ais_2023.parquet
  $(basename "$0") -p 2023-04 ais_parquet/               # April 2023 -> ais_2023-04.parquet
  $(basename "$0") -p 2023 ais_parquet/ archive.parquet  # explicit output path
  $(basename "$0") --verify ais_parquet/                 # confirm row counts match
EOF
}

POSITIONAL=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--period)        PERIOD="$2";          shift 2 ;;
    -c|--compression)   CODEC="$2";           shift 2 ;;
    --row-group-size)   ROW_GROUP_SIZE="$2";  shift 2 ;;
    --verify)           VERIFY=1;             shift ;;
    -h|--help)          usage; exit 0 ;;
    --)                 shift; POSITIONAL+=("$@"); break ;;
    -*)                 echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
    *)                  POSITIONAL+=("$1");   shift ;;
  esac
done

if [[ ${#POSITIONAL[@]} -lt 1 ]]; then
  echo "Error: input directory is required" >&2
  usage >&2
  exit 2
fi

IN_DIR="${POSITIONAL[0]%/}"

if [[ ! -d "$IN_DIR" ]]; then
  echo "Error: '$IN_DIR' is not a directory" >&2
  exit 1
fi

case "$CODEC" in
  zstd|snappy|gzip|lz4|uncompressed) ;;
  *) echo "Error: unknown compression codec '$CODEC'" >&2; exit 2 ;;
esac

if ! command -v duckdb >/dev/null 2>&1; then
  echo "Error: duckdb CLI not found." >&2
  echo "Install: brew install duckdb  (macOS) or https://duckdb.org/docs/installation/" >&2
  exit 1
fi

# Default output filename derived from input dir + optional period.
default_out_name() {
  local base
  base=$(basename "$IN_DIR")
  base="${base%_parquet}"
  if [[ -n "$PERIOD" ]]; then
    printf '%s_%s.parquet' "$base" "$PERIOD"
  else
    printf '%s.parquet' "$base"
  fi
}

OUT_FILE="${POSITIONAL[1]:-$(default_out_name)}"

human_bytes() {
  awk -v b="$1" 'BEGIN {
    split("B KB MB GB TB PB", u, " ")
    i = 1
    while (b >= 1024 && i < 6) { b /= 1024; i++ }
    if (i == 1) printf "%d %s", b, u[i]
    else        printf "%.1f %s", b, u[i]
  }'
}

human_duration() {
  awk -v s="$1" 'BEGIN {
    if (s < 60)    { printf "%.1fs", s; exit }
    if (s < 3600)  { printf "%dm%02ds", int(s/60), int(s) % 60; exit }
    printf "%dh%02dm", int(s/3600), int((s % 3600) / 60)
  }'
}

filesize() {
  stat -f%z "$1" 2>/dev/null || stat -c%s "$1" 2>/dev/null || echo 0
}

# Choose the glob pattern based on --period filter.
if [[ -n "$PERIOD" ]]; then
  GLOB_PATTERN="*_${PERIOD}*.parquet"
else
  GLOB_PATTERN="*.parquet"
fi

# Expand the glob to count and total input files.
shopt -s nullglob
INPUT_FILES=("$IN_DIR"/$GLOB_PATTERN)
shopt -u nullglob
NFILES=${#INPUT_FILES[@]}

if (( NFILES == 0 )); then
  if [[ -n "$PERIOD" ]]; then
    echo "Error: no files match $IN_DIR/$GLOB_PATTERN" >&2
  else
    echo "Error: no .parquet files found in $IN_DIR/" >&2
  fi
  exit 1
fi

INPUT_BYTES=0
for f in "${INPUT_FILES[@]}"; do
  INPUT_BYTES=$((INPUT_BYTES + $(filesize "$f")))
done

if [[ -e "$OUT_FILE" ]]; then
  echo "Warning: $OUT_FILE already exists and will be overwritten." >&2
fi

if [[ -n "$PERIOD" ]]; then
  echo "Merging $NFILES file(s) matching '$GLOB_PATTERN' from $IN_DIR/ ($(human_bytes "$INPUT_BYTES")) into $OUT_FILE"
else
  echo "Merging $NFILES file(s) from $IN_DIR/ ($(human_bytes "$INPUT_BYTES")) into $OUT_FILE"
fi
echo "  codec: $CODEC${ROW_GROUP_SIZE:+, row group size: $ROW_GROUP_SIZE}"
echo

# Build the COPY options list.
COPY_OPTS="FORMAT PARQUET, COMPRESSION '${CODEC}'"
if [[ -n "$ROW_GROUP_SIZE" ]]; then
  COPY_OPTS="${COPY_OPTS}, ROW_GROUP_SIZE ${ROW_GROUP_SIZE}"
fi

DUCKDB_GLOB="${IN_DIR}/${GLOB_PATTERN}"

START=$(date +%s)
duckdb -c "COPY (SELECT * FROM read_parquet('${DUCKDB_GLOB}')) TO '${OUT_FILE}' (${COPY_OPTS});"
END=$(date +%s)
DUR=$((END - START))

OUT_BYTES=$(filesize "$OUT_FILE")
RATIO=$(awk -v i="$INPUT_BYTES" -v o="$OUT_BYTES" 'BEGIN { if (i==0) print "n/a"; else printf "%.2f", o/i }')

echo "Wrote $OUT_FILE  ($(human_bytes "$OUT_BYTES"), ratio ${RATIO}x of input) in $(human_duration "$DUR")"

if (( VERIFY )); then
  echo
  echo "Verifying row counts..."
  IN_ROWS=$(duckdb -noheader -list -c "SELECT count(*) FROM read_parquet('${DUCKDB_GLOB}');" | tr -d ' ')
  OUT_ROWS=$(duckdb -noheader -list -c "SELECT count(*) FROM read_parquet('${OUT_FILE}');" | tr -d ' ')
  echo "  input:  ${IN_ROWS} rows"
  echo "  output: ${OUT_ROWS} rows"
  if [[ "$IN_ROWS" != "$OUT_ROWS" ]]; then
    echo "ERROR: row count mismatch" >&2
    exit 1
  fi
  echo "  OK"
fi
