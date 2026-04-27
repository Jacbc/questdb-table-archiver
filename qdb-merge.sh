#!/usr/bin/env bash
#
# qdb-merge.sh — merge a directory of chunked Parquet files into one file.
#
# Companion to qdb-export.sh. Uses DuckDB's read_parquet + COPY to stream
# all input files into a single output Parquet without loading everything
# into memory. The chunk filenames produced by qdb-export.sh sort in
# chronological order lexicographically, so glob order is already correct.
#
# Requires: bash 4+, duckdb CLI (brew install duckdb).

set -euo pipefail

CODEC="zstd"
VERIFY=0
ROW_GROUP_SIZE=""

usage() {
  cat <<EOF
Usage: $(basename "$0") [options] <input-dir> [output-file]

Merge all *.parquet files in <input-dir> into a single Parquet file.

Options:
  -c, --compression CODEC   zstd|snappy|gzip|lz4|uncompressed (default: zstd)
      --row-group-size N    Target row group size (default: DuckDB default)
      --verify              Compare row counts of input vs output after merge
  -h, --help                Show this help

Arguments:
  input-dir     Directory containing chunked .parquet files
  output-file   Output path (default: <input-dir>.parquet)

Examples:
  $(basename "$0") ais_parquet/                            # -> ais_parquet.parquet
  $(basename "$0") ais_parquet/ ais_full.parquet
  $(basename "$0") -c snappy ais_parquet/ ais_full.parquet
  $(basename "$0") --verify ais_parquet/
EOF
}

POSITIONAL=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -c|--compression)   CODEC="$2";          shift 2 ;;
    --row-group-size)   ROW_GROUP_SIZE="$2"; shift 2 ;;
    --verify)           VERIFY=1;            shift ;;
    -h|--help)          usage; exit 0 ;;
    --)                 shift; POSITIONAL+=("$@"); break ;;
    -*)                 echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
    *)                  POSITIONAL+=("$1");  shift ;;
  esac
done

if [[ ${#POSITIONAL[@]} -lt 1 ]]; then
  echo "Error: input directory is required" >&2
  usage >&2
  exit 2
fi

IN_DIR="${POSITIONAL[0]%/}"
OUT_FILE="${POSITIONAL[1]:-${IN_DIR}.parquet}"

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

# Count input files (handle empty glob gracefully).
shopt -s nullglob
INPUT_FILES=("$IN_DIR"/*.parquet)
shopt -u nullglob
NFILES=${#INPUT_FILES[@]}

if (( NFILES == 0 )); then
  echo "Error: no .parquet files found in $IN_DIR/" >&2
  exit 1
fi

INPUT_BYTES=0
for f in "${INPUT_FILES[@]}"; do
  INPUT_BYTES=$((INPUT_BYTES + $(filesize "$f")))
done

if [[ -e "$OUT_FILE" ]]; then
  echo "Warning: $OUT_FILE already exists and will be overwritten." >&2
fi

echo "Merging $NFILES file(s) from $IN_DIR/ ($(human_bytes "$INPUT_BYTES")) into $OUT_FILE"
echo "  codec: $CODEC${ROW_GROUP_SIZE:+, row group size: $ROW_GROUP_SIZE}"
echo

# Build the COPY options list.
COPY_OPTS="FORMAT PARQUET, COMPRESSION '${CODEC}'"
if [[ -n "$ROW_GROUP_SIZE" ]]; then
  COPY_OPTS="${COPY_OPTS}, ROW_GROUP_SIZE ${ROW_GROUP_SIZE}"
fi

START=$(date +%s)
duckdb -c "COPY (SELECT * FROM read_parquet('${IN_DIR}/*.parquet')) TO '${OUT_FILE}' (${COPY_OPTS});"
END=$(date +%s)
DUR=$((END - START))

OUT_BYTES=$(filesize "$OUT_FILE")
RATIO=$(awk -v i="$INPUT_BYTES" -v o="$OUT_BYTES" 'BEGIN { if (i==0) print "n/a"; else printf "%.2f", o/i }')

echo "Wrote $OUT_FILE  ($(human_bytes "$OUT_BYTES"), ratio ${RATIO}x of input) in $(human_duration "$DUR")"

if (( VERIFY )); then
  echo
  echo "Verifying row counts..."
  IN_ROWS=$(duckdb -noheader -list -c "SELECT count(*) FROM read_parquet('${IN_DIR}/*.parquet');" | tr -d ' ')
  OUT_ROWS=$(duckdb -noheader -list -c "SELECT count(*) FROM read_parquet('${OUT_FILE}');" | tr -d ' ')
  echo "  input:  ${IN_ROWS} rows"
  echo "  output: ${OUT_ROWS} rows"
  if [[ "$IN_ROWS" != "$OUT_ROWS" ]]; then
    echo "ERROR: row count mismatch" >&2
    exit 1
  fi
  echo "  OK"
fi
