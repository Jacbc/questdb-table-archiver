#!/usr/bin/env bash
#
# qdb-backup-setup.sh: set up a qdb-backup.py backup on this machine, in one go.
#
#   ./qdb-backup-setup.sh BACKUP_DIR QDB_URL [init options]
#
#   e.g. ./qdb-backup-setup.sh ~/qdb-backup http://questdb.example:9000 --group sensor_=sensors --start 2026-01
#
# 1. checks python3 >= 3.8 and curl
# 2. installs a pinned DuckDB CLI into ~/.local/bin (sha256-checked; Linux x86_64/aarch64) unless one is there
#    (a snap-installed duckdb is not used: snaps are sandboxed away from /mnt and network shares)
# 3. writes BACKUP_DIR/qdb-backup.config.json with `qdb-backup.py init` (every table, mode chosen from its definition);
#    an existing qdb-backup.config.json is kept (pass --force to write it again)
# 4. offers to move existing qdb-export.sh folders (<prefix>_parquet/ next to this script) into BACKUP_DIR, so their
#    files are taken over instead of downloaded again
# 5. writes BACKUP_DIR/run-backup.sh: the one command for every later run
#
# Init options (passed on): --group NAME_PREFIX=DIR (repeatable), --start YYYY-MM, --full-below N, --force.

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
DUCK_VERSION="v1.4.1"
SHA256_amd64="3e29952507ebd202e8d0e2678df4490689bfc9c86534e240b541791b969df0e1"
SHA256_arm64="646f60c92268c441f0095ce855e16528a8539e466da2788649f359be8cbdc8d1"

usage() { sed -n '3,19p' "$0" | sed 's/^# \{0,1\}//'; }
[[ $# -ge 2 && "$1" != -* && "$2" != -* ]] || { usage >&2; exit 2; }
ROOT="$1"; URL="$2"; shift 2
die() { echo "setup: $*" >&2; exit 1; }

# ---- 1. tools -----------------------------------------------------------------------------------------------------
python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 8) else 1)' 2>/dev/null || die "python3 >= 3.8 is required"
command -v curl >/dev/null || die "curl is required"
curl -fsS -m 10 -G "$URL/exec" --data-urlencode "query=SELECT 1" >/dev/null || die "no QuestDB answers at $URL"
echo "QuestDB at $URL answers"

# ---- 2. DuckDB ----------------------------------------------------------------------------------------------------
DUCK="${DUCKDB:-}"
if [[ -z "$DUCK" && -x "$HOME/.local/bin/duckdb" ]]; then DUCK="$HOME/.local/bin/duckdb"; fi
if [[ -z "$DUCK" ]] && command -v duckdb >/dev/null; then
  DUCK="$(command -v duckdb)"
  # A snap is sandboxed: it usually cannot read /mnt, /media or network shares. Install the pinned one instead.
  if [[ "$(readlink -f "$DUCK")" == /snap/* || "$DUCK" == /snap/* ]]; then
    echo "not using $DUCK (a snap: it may not reach $ROOT)"
    DUCK=""
  fi
fi
if [[ -z "$DUCK" ]]; then
  case "$(uname -s)/$(uname -m)" in
    Linux/x86_64) ARCH=amd64 ;;
    Linux/aarch64|Linux/arm64) ARCH=arm64 ;;
    *) die "install the DuckDB CLI yourself (https://duckdb.org/docs/installation/), then run this again" ;;
  esac
  WANT_VAR="SHA256_${ARCH}"; WANT="${!WANT_VAR}"
  DUCK_URL="https://github.com/duckdb/duckdb/releases/download/${DUCK_VERSION}/duckdb_cli-linux-${ARCH}.zip"
  TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
  echo "installing DuckDB ${DUCK_VERSION} (${ARCH}) into ~/.local/bin"
  curl -fsSL -o "$TMP/duckdb.zip" "$DUCK_URL"
  GOT=$( (sha256sum "$TMP/duckdb.zip" 2>/dev/null || shasum -a 256 "$TMP/duckdb.zip") | awk '{print $1}')
  [[ "$GOT" == "$WANT" ]] || die "sha256 mismatch for $DUCK_URL (expected $WANT, got $GOT)"
  python3 -I -c 'import sys, zipfile; zipfile.ZipFile(sys.argv[1]).extract("duckdb", sys.argv[2])' "$TMP/duckdb.zip" "$TMP"
  mkdir -p "$HOME/.local/bin"
  install -m 0755 "$TMP/duckdb" "$HOME/.local/bin/duckdb"
  DUCK="$HOME/.local/bin/duckdb"
fi
"$DUCK" -c "SELECT 1" >/dev/null 2>&1 || die "DuckDB at $DUCK does not run"
echo "DuckDB $("$DUCK" --version | awk '{print $1}') at $DUCK"

# ---- 3. config ----------------------------------------------------------------------------------------------------
mkdir -p "$ROOT"
ROOT="$(cd "$ROOT" && pwd)"
FORCE=""
for a in "$@"; do [[ "$a" == --force ]] && FORCE=1; done
if [[ -f "$ROOT/qdb-backup.config.json" && -z "$FORCE" ]]; then
  echo "keeping the existing $ROOT/qdb-backup.config.json (--force writes it again)"
else
  echo
  python3 "$HERE/qdb-backup.py" init --root "$ROOT" --url "$URL" "$@"
fi

# ---- 4. existing qdb-export.sh folders ----------------------------------------------------------------------------
echo
while IFS=$'\t' read -r name dir; do
  src="$HERE/$name"
  if [[ "$dir" == "." ]]; then dst="$ROOT/$name"; else dst="$ROOT/$dir/$name"; fi
  [[ -d "$src" && ! -e "$dst" ]] || continue
  n=$(find "$src" -maxdepth 1 -name '*.parquet' | wc -l | tr -d ' ')
  ans=""
  if [[ -r /dev/tty ]]; then
    read -r -p "Move $src ($n files) to $dst, so its files are taken over instead of downloaded again? [y/N] " ans </dev/tty || ans=""
  fi
  if [[ "$ans" == y || "$ans" == Y ]]; then
    mkdir -p "$(dirname "$dst")"
    mv "$src" "$dst"   # mv keeps the file times
    echo "  moved"
  else
    echo "  left in place (move it yourself with: mv \"$src\" \"$dst\")"
  fi
done < <(python3 - "$ROOT/qdb-backup.config.json" <<'PY'
import json, sys
for t in json.load(open(sys.argv[1]))["tables"]:
    if t.get("mode", "chunked") == "chunked":
        print("%s_parquet\t%s" % (t.get("prefix", t["table"]), t.get("dir", ".")))
PY
)

# ---- 5. run script ------------------------------------------------------------------------------------------------
q() { printf '%q' "$1"; }
cat > "$ROOT/run-backup.sh" <<EOF
#!/usr/bin/env bash
# Written by qdb-backup-setup.sh. Back up what changed: ./run-backup.sh [--dry-run] [run options]
exec python3 $(q "$HERE/qdb-backup.py") run --config $(q "$ROOT/qdb-backup.config.json") --root $(q "$ROOT") \\
  --url $(q "$URL") --duckdb $(q "$DUCK") "\$@"
EOF
chmod +x "$ROOT/run-backup.sh"

cat <<EOF

Set up. Next:
  $ROOT/run-backup.sh --dry-run     shows the plan, downloads nothing (read it before the first real run)
  $ROOT/run-backup.sh               the real run (and every run after it)
EOF
