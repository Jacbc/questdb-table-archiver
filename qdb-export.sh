#!/usr/bin/env bash
#
# qdb-export.sh — chunked Parquet export for QuestDB
#
# Works around QuestDB's ~5 min HTTP idle timeout that breaks single-stream
# /exp?fmt=parquet exports of large tables, by issuing many smaller queries
# bounded with `WHERE <ts> IN '<period>'` and writing one Parquet file per
# chunk. Each file is independently valid; the resulting directory can be
# read as a single dataset by DuckDB / Polars / pandas.
#
# Requires: bash 4+, curl, python3 (used only for date math + JSON parsing).

set -euo pipefail

# ---------- defaults --------------------------------------------------------

QDB="${QDB:-http://localhost:9000}"
TIMEOUT="${TIMEOUT:-280}"
OUT_DIR="${OUT_DIR:-}"
TABLE="${QDB_TABLE:-}"
TS_COL="${QDB_TS_COL:-}"
BY="${QDB_BY:-month}"

# ---------- usage -----------------------------------------------------------

usage() {
  cat <<EOF
Usage: $(basename "$0") [options] [period...]

Export a QuestDB table to chunked Parquet files via the REST /exp endpoint.

Options:
  -t, --table NAME       Table to export                       (env: QDB_TABLE)
  -c, --ts-col NAME      Timestamp column (auto-detected)      (env: QDB_TS_COL)
  -b, --by GRANULARITY   Chunk size: year|month|week|day       (default: month)
  -o, --out DIR          Output directory                      (default: <table>_parquet)
  -u, --url URL          QuestDB base URL                      (env: QDB, default: http://localhost:9000)
      --timeout SECS     Per-chunk curl timeout in seconds     (env: TIMEOUT, default: 280)
  -h, --help             Show this help

Periods:
  If no periods are given, the script auto-detects the data range with
  SELECT MIN, MAX FROM <table> and iterates from start to end at --by.

  Period formats:
    YYYY         full year   (expanded into chunks at --by granularity)
    YYYY-MM      single month
    YYYY-Www     ISO week
    YYYY-MM-DD   single day

Examples:
  $(basename "$0") --table ais 2026-04
  $(basename "$0") --table ais --by month 2025
  $(basename "$0") --table trades --by day 2026-04-27
  $(basename "$0") --table sensors                      # auto-detect everything
  QDB=http://192.168.1.110:9000 $(basename "$0") -t ais 2024 2025 2026
EOF
}

# ---------- arg parsing -----------------------------------------------------

PERIODS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--table)   TABLE="$2";   shift 2 ;;
    -c|--ts-col)  TS_COL="$2";  shift 2 ;;
    -b|--by)      BY="$2";      shift 2 ;;
    -o|--out)     OUT_DIR="$2"; shift 2 ;;
    -u|--url)     QDB="$2";     shift 2 ;;
    --timeout)    TIMEOUT="$2"; shift 2 ;;
    -h|--help)    usage; exit 0 ;;
    --)           shift; PERIODS+=("$@"); break ;;
    -*)           echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
    *)            PERIODS+=("$1"); shift ;;
  esac
done

if [[ -z "$TABLE" ]]; then
  echo "Error: --table is required (or set QDB_TABLE)" >&2
  usage >&2
  exit 2
fi

case "$BY" in
  year|month|week|day) ;;
  *) echo "Error: --by must be one of: year month week day" >&2; exit 2 ;;
esac

OUT_DIR="${OUT_DIR:-${TABLE}_parquet}"

# ---------- helpers ---------------------------------------------------------

urlencode() {
  python3 -c 'import urllib.parse, sys; print(urllib.parse.quote(sys.stdin.read(), safe=""))'
}

# Run a SELECT via /exec, echo the JSON response.
qdb_exec() {
  local sql="$1" encoded
  encoded=$(printf '%s' "$sql" | urlencode)
  curl -fsS --max-time 30 "${QDB}/exec?query=${encoded}"
}

# Pull dataset[row][col] from a JSON response on stdin.
json_cell() {
  local row="$1" col="$2"
  python3 -c "
import json, sys
d = json.load(sys.stdin)
ds = d.get('dataset') or []
if not ds:
    sys.exit(1)
v = ds[$row][$col]
print('' if v is None else v)
"
}

# Resolve the timestamp column for a table.
# Strategy:
#   1. Use the designated timestamp from tables() if one is set.
#   2. Otherwise scan table_columns() for a TIMESTAMP-typed column whose
#      name matches a common convention (ts, time, timestamp, event_time,
#      created_at, dt, date).
#   3. Otherwise pick the first TIMESTAMP-typed column on the table.
# Returns the column name on stdout, or non-zero exit if nothing matches.
detect_ts_col() {
  local table="$1"

  local designated
  designated=$(qdb_exec "SELECT designatedTimestamp FROM tables() WHERE table_name = '${table}'" \
               | json_cell 0 0 2>/dev/null || true)
  if [[ -n "$designated" ]]; then
    printf '%s' "$designated"
    return 0
  fi

  qdb_exec "SELECT column, type FROM table_columns('${table}')" | python3 -c '
import json, sys

CANDIDATES = ["ts", "time", "timestamp", "event_time", "created_at", "dt", "date"]

d = json.load(sys.stdin)
cols = [(row[0], row[1]) for row in (d.get("dataset") or [])]
ts_cols = [name for name, t in cols if str(t).upper() == "TIMESTAMP"]

lower_to_actual = {name.lower(): name for name in ts_cols}
for cand in CANDIDATES:
    if cand in lower_to_actual:
        print(lower_to_actual[cand])
        sys.exit(0)
if ts_cols:
    print(ts_cols[0])
    sys.exit(0)
sys.exit(1)
'
}

detect_range() {
  local table="$1" ts="$2"
  qdb_exec "SELECT min(${ts}), max(${ts}) FROM ${table}" \
    | python3 -c '
import json, sys
d = json.load(sys.stdin)
ds = d.get("dataset") or []
if not ds or ds[0][0] is None:
    sys.exit(1)
print(ds[0][0], ds[0][1])
'
}

# Generate periods at <by> granularity covering [start, end] inclusive.
gen_periods() {
  local start="$1" end="$2" by="$3"
  python3 - "$start" "$end" "$by" <<'PY'
import sys
from datetime import datetime, timedelta

def parse(s):
    s = s.replace('Z', '').split('.')[0]
    return datetime.fromisoformat(s[:19])

start, end, by = parse(sys.argv[1]), parse(sys.argv[2]), sys.argv[3]

if by == 'year':
    for y in range(start.year, end.year + 1):
        print(f'{y:04d}')
elif by == 'month':
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        print(f'{y:04d}-{m:02d}')
        m += 1
        if m > 12:
            m, y = 1, y + 1
elif by == 'week':
    d = start.date() - timedelta(days=start.isoweekday() - 1)
    while d <= end.date():
        iy, iw, _ = d.isocalendar()
        print(f'{iy:04d}-W{iw:02d}')
        d += timedelta(days=7)
elif by == 'day':
    d = start.date()
    while d <= end.date():
        print(d.isoformat())
        d += timedelta(days=1)
PY
}

# Expand a user-supplied period (YYYY, YYYY-MM, YYYY-Www, YYYY-MM-DD)
# into chunks at --by granularity.
expand_period() {
  local period="$1" by="$2"
  python3 - "$period" "$by" <<'PY'
import sys, re
from datetime import date, timedelta

period, by = sys.argv[1], sys.argv[2]

if re.fullmatch(r'\d{4}', period):
    year = int(period)
    if by == 'year':
        print(f'{year:04d}')
    elif by == 'month':
        for m in range(1, 13):
            print(f'{year:04d}-{m:02d}')
    elif by == 'week':
        d = date(year, 1, 1)
        d -= timedelta(days=d.isoweekday() - 1)
        while True:
            iy, iw, _ = d.isocalendar()
            if iy > year:
                break
            if iy == year:
                print(f'{iy:04d}-W{iw:02d}')
            d += timedelta(days=7)
    elif by == 'day':
        d = date(year, 1, 1)
        while d.year == year:
            print(d.isoformat())
            d += timedelta(days=1)
elif re.fullmatch(r'\d{4}-\d{2}', period):
    if by == 'day':
        y, m = map(int, period.split('-'))
        d = date(y, m, 1)
        while d.month == m:
            print(d.isoformat())
            d += timedelta(days=1)
    else:
        print(period)
elif re.fullmatch(r'\d{4}-W\d{2}', period) or re.fullmatch(r'\d{4}-\d{2}-\d{2}', period):
    print(period)
else:
    print(f'Invalid period: {period}', file=sys.stderr)
    sys.exit(1)
PY
}

is_valid_parquet() {
  local f="$1" magic
  [[ -s "$f" ]] || return 1
  magic=$(tail -c 4 "$f" 2>/dev/null || true)
  [[ "$magic" == "PAR1" ]]
}

filesize() {
  stat -f%z "$1" 2>/dev/null || stat -c%s "$1" 2>/dev/null || echo 0
}

# Format bytes as human-readable (e.g. "45.2 MB").
human_bytes() {
  awk -v b="$1" 'BEGIN {
    split("B KB MB GB TB PB", u, " ")
    i = 1
    while (b >= 1024 && i < 6) { b /= 1024; i++ }
    if (i == 1) printf "%d %s", b, u[i]
    else        printf "%.1f %s", b, u[i]
  }'
}

# Format seconds as a compact duration (e.g. "8.3s", "2m14s", "1h08m").
human_duration() {
  awk -v s="$1" 'BEGIN {
    if (s < 60)    { printf "%.1fs", s; exit }
    if (s < 3600)  { printf "%dm%02ds", int(s/60), int(s) % 60; exit }
    printf "%dh%02dm", int(s/3600), int((s % 3600) / 60)
  }'
}

# Set by fetch_period for the main loop to consume.
LAST_SIZE=0
LAST_DURATION=0

# Returns 0 ok / 1 fail / 2 empty.
# Args: $1 = period, $2 = chunk index (e.g. "12"), $3 = total chunks
fetch_period() {
  local period="$1" idx="$2" total="$3"
  local out="${OUT_DIR}/${TABLE}_${period}.parquet"
  local err="${out}.err"
  local prefix
  prefix=$(printf "[%${#total}d/%d]" "$idx" "$total")

  LAST_SIZE=0
  LAST_DURATION=0

  if is_valid_parquet "$out"; then
    local sz; sz=$(filesize "$out")
    LAST_SIZE=$sz
    printf "%s  skip    %-12s %10s  (already complete)\n" \
      "$prefix" "$period" "$(human_bytes "$sz")"
    return 0
  fi

  local sql="SELECT * FROM ${TABLE} WHERE ${TS_COL} IN '${period}'"
  local encoded
  encoded=$(printf '%s' "$sql" | urlencode)
  local url="${QDB}/exp?fmt=parquet&query=${encoded}"

  rm -f "$err"
  local rc=0
  local t0 t1 dur
  t0=$(date +%s)
  curl -fsS --max-time "$TIMEOUT" -o "$out" "$url" 2> "$err" || rc=$?
  t1=$(date +%s)
  dur=$((t1 - t0))
  LAST_DURATION=$dur

  if (( rc != 0 )); then
    printf "%s  FAIL    %-12s            after %s  (curl exit %d; see %s)\n" \
      "$prefix" "$period" "$(human_duration "$dur")" "$rc" "$err"
    return 1
  fi

  if ! is_valid_parquet "$out"; then
    mv -f "$out" "${out}.broken"
    printf "%s  BROKEN  %-12s            after %s  (no PAR1 trailer; renamed .broken)\n" \
      "$prefix" "$period" "$(human_duration "$dur")"
    return 1
  fi

  local size; size=$(filesize "$out")

  # Schema-only Parquet (no rows) is small. Confirm with a count query and
  # delete if truly empty so the archive doesn't fill with zero-row files.
  if (( size < 2048 )); then
    local cnt
    cnt=$(qdb_exec "SELECT count() FROM ${TABLE} WHERE ${TS_COL} IN '${period}'" \
          | json_cell 0 0 2>/dev/null || echo 0)
    if [[ "$cnt" == "0" ]]; then
      rm -f "$out"
      printf "%s  empty   %-12s            (no rows; deleted)\n" \
        "$prefix" "$period"
      return 2
    fi
  fi

  rm -f "$err"
  LAST_SIZE=$size
  local rate=""
  if (( dur > 0 )); then
    rate=$(awk -v b="$size" -v s="$dur" 'BEGIN { printf "%.1f MB/s", (b/1048576)/s }')
    rate="  ${rate}"
  fi
  printf "%s  ok      %-12s %10s  in %6s%s\n" \
    "$prefix" "$period" "$(human_bytes "$size")" "$(human_duration "$dur")" "$rate"
  return 0
}

# ---------- main ------------------------------------------------------------

mkdir -p "$OUT_DIR"

if [[ -z "$TS_COL" ]]; then
  echo "Detecting timestamp column for table '${TABLE}'..."
  TS_COL=$(detect_ts_col "$TABLE" || true)
  if [[ -z "$TS_COL" ]]; then
    echo "Error: could not detect designated timestamp column for '${TABLE}'." >&2
    echo "Pass it explicitly with --ts-col." >&2
    exit 1
  fi
  echo "  -> ${TS_COL}"
fi

CHUNKS=()
if [[ ${#PERIODS[@]} -eq 0 ]]; then
  echo "Detecting data range for ${TABLE}.${TS_COL}..."
  if ! read -r RANGE_MIN RANGE_MAX < <(detect_range "$TABLE" "$TS_COL"); then
    echo "Error: could not detect data range for ${TABLE}.${TS_COL} (table empty?)." >&2
    exit 1
  fi
  echo "  -> ${RANGE_MIN}  ..  ${RANGE_MAX}"
  while IFS= read -r p; do CHUNKS+=("$p"); done < <(gen_periods "$RANGE_MIN" "$RANGE_MAX" "$BY")
else
  for p in "${PERIODS[@]}"; do
    while IFS= read -r c; do CHUNKS+=("$c"); done < <(expand_period "$p" "$BY")
  done
fi

if [[ ${#CHUNKS[@]} -eq 0 ]]; then
  echo "No chunks to export."
  exit 0
fi

TOTAL_CHUNKS=${#CHUNKS[@]}
PROGRESS_EVERY="${PROGRESS_EVERY:-10}"

echo
echo "Exporting ${TOTAL_CHUNKS} chunk(s) of '${TABLE}' to ${OUT_DIR}/ at granularity '${BY}'"
echo

OK=0; FAIL=0; EMPTY=0
TOTAL_BYTES=0
FAILED_CHUNKS=()
RUN_START=$(date +%s)

for i in "${!CHUNKS[@]}"; do
  period="${CHUNKS[$i]}"
  idx=$((i + 1))

  set +e
  fetch_period "$period" "$idx" "$TOTAL_CHUNKS"
  rc=$?
  set -e

  case $rc in
    0) OK=$((OK + 1)) ;;
    1) FAIL=$((FAIL + 1)); FAILED_CHUNKS+=("$period") ;;
    2) EMPTY=$((EMPTY + 1)) ;;
  esac
  TOTAL_BYTES=$((TOTAL_BYTES + LAST_SIZE))

  # Periodic progress line: every N chunks, and always on the last chunk.
  if (( idx % PROGRESS_EVERY == 0 )) || (( idx == TOTAL_CHUNKS )); then
    NOW=$(date +%s)
    ELAPSED=$((NOW - RUN_START))
    REMAINING=$((TOTAL_CHUNKS - idx))
    PCT=$(awk -v i="$idx" -v t="$TOTAL_CHUNKS" 'BEGIN { printf "%.1f", (i/t)*100 }')
    if (( idx > 0 && REMAINING > 0 )); then
      ETA=$(( ELAPSED * REMAINING / idx ))
      ETA_STR=" · eta ~$(human_duration "$ETA")"
    else
      ETA_STR=""
    fi
    printf "  ── %d/%d (%s%%) · %s archived · elapsed %s%s ──\n" \
      "$idx" "$TOTAL_CHUNKS" "$PCT" \
      "$(human_bytes "$TOTAL_BYTES")" \
      "$(human_duration "$ELAPSED")" \
      "$ETA_STR"
  fi
done

RUN_END=$(date +%s)
RUN_ELAPSED=$((RUN_END - RUN_START))

echo
echo "Done: ${OK} ok, ${FAIL} failed, ${EMPTY} empty  (in $(human_duration "$RUN_ELAPSED"))"
TOTAL=$(du -sh "$OUT_DIR" 2>/dev/null | awk '{print $1}')
echo "Archive: ${OUT_DIR}/  (${TOTAL})"

if (( FAIL > 0 )); then
  echo
  echo "Failed chunks (rerun the script to retry):"
  for p in "${FAILED_CHUNKS[@]}"; do echo "  ${p}"; done
  exit 1
fi
