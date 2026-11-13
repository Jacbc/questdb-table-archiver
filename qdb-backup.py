#!/usr/bin/env python3
"""
qdb-backup.py: a rebuildable Parquet backup of a QuestDB database.

Subcommands
  run         export the configured tables (monthly Parquet files, only what changed), capture the schema,
              write the manifests, BACKUP.json and SHA256SUMS
  merge-year  merge the monthly files of one year into <prefix>_<YYYY>.parquet (DuckDB), verified per day
  verify      check the files of a backup against its manifests (sha256; --deep adds DuckDB fingerprints)
  restore     rebuild a working QuestDB from a backup: tables, data, DEDUP, views, then verify everything
  schema      capture the schema only

Requires Python 3.8+ (standard library only) and, for run and merge-year, the DuckDB CLI: it checks every file
locally against the source (run --no-duckdb skips that, explicitly). QuestDB itself is the restore engine
(read_parquet).

See README.md, "Full backup and restore".
"""

import argparse
import calendar
import datetime as dt
import errno
import hashlib
import http.client
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

try:
    import fcntl  # POSIX only
except ImportError:  # pragma: no cover
    fcntl = None

FORMAT = 1
P = 1000000007
DAY_US = 86400 * 1000000
DEFAULT_MAX_UNCOMMITTED_ROWS = 500000
DEFAULT_O3_MAX_LAG_US = 600000000
# Types that come back unchanged (or castable without loss) after DuckDB rewrites a QuestDB-written file.
# Needed for merge-year (DuckDB writes the yearly file).
DUCKDB_SAFE_TYPES = {"BOOLEAN", "BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "TIMESTAMP", "DATE",
                     "SYMBOL", "STRING", "VARCHAR"}
# CHAR is left out: a NULL CHAR is written as code 0, which DuckDB counts as a value.
SCALAR_FP_TYPES = {"BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "TIMESTAMP", "TIMESTAMP_NS", "DATE",
                   "STRING", "VARCHAR", "SYMBOL"}
TABLES_WHITELIST = ["designatedTimestamp", "partitionBy", "walEnabled", "dedup", "ttlValue", "ttlUnit",
                    "maxUncommittedRows", "o3MaxLag"]
COLUMNS_WHITELIST = ["column", "type", "indexed", "indexBlockCapacity", "symbolCached", "symbolCapacity",
                     "designated", "upsertKey"]


# ---------------------------------------------------------------------------------------------------------------
# small helpers


class Failure(Exception):
    """A chunk or table failed; the run carries on with the next one."""


def log(msg=""):
    print(msg, flush=True)


def now_us():
    return int(time.time() * 1000000)


def iso_now():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_qts(s):
    """QuestDB timestamp text ('2026-11-07T04:42:00.000000Z') -> microseconds since the epoch (UTC)."""
    if s is None:
        return None
    s = str(s).rstrip("Z")
    frac = "0"
    if "." in s:
        s, frac = s.split(".", 1)
        frac = (frac + "000000")[:6]
    t = dt.datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
    return calendar.timegm(t.timetuple()) * 1000000 + int(frac)


def us_to_iso(us):
    if us is None:
        return None
    return (dt.datetime(1970, 1, 1) + dt.timedelta(microseconds=us)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def day_of(us):
    return (dt.datetime(1970, 1, 1) + dt.timedelta(microseconds=us)).strftime("%Y-%m-%d")


def day_from_number(n):
    return (dt.date(1970, 1, 1) + dt.timedelta(days=int(n))).isoformat()


def day_start_us(day):
    d = dt.date.fromisoformat(day)
    return calendar.timegm(d.timetuple()) * 1000000


def month_start_us(month):
    return day_start_us(month + "-01")


def month_end_us(month):
    y, m = int(month[:4]), int(month[5:7])
    nxt = "%04d-%02d-01" % ((y + 1, 1) if m == 12 else (y, m + 1))
    return day_start_us(nxt)


def year_end_us(year):
    return day_start_us("%04d-01-01" % (int(year) + 1))


def day_range(d0, d1):
    a, b = dt.date.fromisoformat(d0), dt.date.fromisoformat(d1)
    out = []
    while a <= b:
        out.append(a.isoformat())
        a += dt.timedelta(days=1)
    return out


def qident(name):
    return '"' + name.replace('"', '""') + '"'


def qlit(s):
    return "'" + str(s).replace("'", "''") + "'"


def base_type(t):
    return str(t).split("(")[0].upper()


def to_int(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    s = str(v)
    if re.fullmatch(r"-?\d+", s):
        return int(s)
    return int(float(s))


def human_bytes(n):
    n = float(n or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return ("%d %s" % (n, unit)) if unit == "B" else ("%.1f %s" % (n, unit))
        n /= 1024.0


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(1 << 22)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def is_parquet(path):
    try:
        size = os.path.getsize(path)
        if size < 12:
            return False
        with open(path, "rb") as f:
            head = f.read(4)
            f.seek(-4, os.SEEK_END)
            tail = f.read(4)
        return head == b"PAR1" and tail == b"PAR1"
    except OSError:
        return False


def write_json(path, obj):
    """Write JSON atomically (tmp + rename), so a killed run never leaves a half-written manifest."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=1, sort_keys=True)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def write_text(path, text):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        f.write(text)
    os.replace(tmp, path)


def read_json(path, default=None):
    if not os.path.exists(path):
        return default
    with open(path) as f:
        return json.load(f)


def rm_f(path):
    try:
        os.remove(path)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise


def safe_name(name):
    return re.sub(r"[^A-Za-z0-9_.-]", "_", name)


# ---------------------------------------------------------------------------------------------------------------
# QuestDB over HTTP


class QdbError(Exception):
    pass


class Qdb:
    def __init__(self, url, timeout=300):
        self.url = url.rstrip("/")
        self.timeout = timeout

    def exec(self, sql, timeout=None):
        u = self.url + "/exec?" + urllib.parse.urlencode({"query": sql})
        try:
            with urllib.request.urlopen(u, timeout=timeout or self.timeout) as r:
                d = json.load(r)
        except urllib.error.HTTPError as e:
            body = e.read()
            try:
                d = json.loads(body)
            except ValueError:
                raise QdbError("HTTP %d: %s" % (e.code, body[:300]))
        except (urllib.error.URLError, OSError) as e:
            raise QdbError("QuestDB at %s unreachable: %s" % (self.url, e))
        if isinstance(d, dict) and "error" in d:
            raise QdbError("%s [sql: %s]" % (d["error"], sql[:200]))
        return d

    def rows(self, sql, timeout=None):
        return self.exec(sql, timeout).get("dataset") or []

    def dicts(self, sql, timeout=None):
        d = self.exec(sql, timeout)
        names = [c["name"] for c in d.get("columns") or []]
        return [dict(zip(names, r)) for r in d.get("dataset") or []]

    def columns_of(self, sql):
        d = self.exec(sql)
        return [(c["name"], c["type"]) for c in d.get("columns") or []]

    def download(self, sql, dest, timeout):
        """Stream /exp?fmt=parquet into dest. Raises Failure on any problem (dest is then removed)."""
        u = self.url + "/exp?" + urllib.parse.urlencode({"query": sql, "fmt": "parquet"})
        t0 = time.time()
        n = 0
        try:
            with urllib.request.urlopen(u, timeout=timeout) as r, open(dest, "wb") as f:
                cl = r.headers.get("Content-Length")
                while True:
                    if time.time() - t0 > timeout:
                        raise Failure("download took longer than %d s" % timeout)
                    b = r.read(1 << 20)
                    if not b:
                        break
                    f.write(b)
                    n += len(b)
            if cl is not None and int(cl) != n:
                raise Failure("short download: %d of %s bytes" % (n, cl))
        except urllib.error.HTTPError as e:
            rm_f(dest)
            body = e.read()[:300]
            raise Failure("HTTP %d: %s" % (e.code, body.decode("utf-8", "replace")))
        except (urllib.error.URLError, OSError, http.client.HTTPException) as e:
            rm_f(dest)
            raise Failure("download failed: %s" % e)
        except Failure:
            rm_f(dest)
            raise
        if not is_parquet(dest):
            rm_f(dest)
            raise Failure("downloaded file is not complete Parquet (no PAR1 header/trailer; server-side timeout?)")
        return n, time.time() - t0

    def wait_wal(self, table, limit_s=60):
        """Wait until the table's WAL is applied. Returns True when caught up, False after limit_s.
        Raises Failure if the table is suspended."""
        t0 = time.time()
        while True:
            r = self.rows("SELECT writerTxn, sequencerTxn, suspended FROM wal_tables() WHERE name = %s" % qlit(table))
            if not r:
                return True  # not a WAL table
            w, s, suspended = r[0]
            if suspended:
                raise Failure("table %s is SUSPENDED in QuestDB (wal_tables()); fix it first" % table)
            if w == s:
                return True
            if time.time() - t0 > limit_s:
                return False
            time.sleep(0.5)


# ---------------------------------------------------------------------------------------------------------------
# DuckDB CLI (optional)


class Duck:
    def __init__(self, exe, temp_dir=None, memory_limit=None, threads=None):
        self.exe = exe
        self.temp_dir = temp_dir
        self.memory_limit = memory_limit
        self.threads = threads

    @staticmethod
    def find(explicit=None, **kw):
        exe = explicit or os.environ.get("DUCKDB") or shutil.which("duckdb")
        if not exe or not (os.path.exists(exe) or shutil.which(exe)):
            return None
        return Duck(exe, **kw)

    def _prelude(self):
        s = ["SET TimeZone='UTC';", "SET preserve_insertion_order=true;"]
        if self.temp_dir:
            os.makedirs(self.temp_dir, exist_ok=True)
            s.append("SET temp_directory=%s;" % qlit(self.temp_dir))
        if self.memory_limit:
            s.append("SET memory_limit=%s;" % qlit(self.memory_limit))
        if self.threads:
            s.append("SET threads=%d;" % int(self.threads))
        return "\n".join(s) + "\n"

    def query(self, sql):
        p = subprocess.run([self.exe, "-json"], input=self._prelude() + sql.rstrip().rstrip(";") + ";\n",
                           capture_output=True, text=True)
        if p.returncode != 0:
            raise Failure("duckdb: " + (p.stderr or p.stdout).strip()[:500])
        out = p.stdout.strip()
        return json.loads(out) if out else []

    def execute(self, sql):
        p = subprocess.run([self.exe], input=self._prelude() + sql.rstrip().rstrip(";") + ";\n",
                           capture_output=True, text=True)
        if p.returncode != 0:
            raise Failure("duckdb: " + (p.stderr or p.stdout).strip()[:500])

    @staticmethod
    def file_list(paths):
        return "[" + ", ".join(qlit(p) for p in paths) + "]"


# ---------------------------------------------------------------------------------------------------------------
# Fingerprint: the same numbers from QuestDB (a table or read_parquet) and from DuckDB (a file). Additive: a month
# is the sum of its days. Per day, every sum fits a signed 64-bit integer. Definition in README.md.


def fp_terms(columns, ts, ts_type="TIMESTAMP"):
    """[(name, questdb_sql, duckdb_sql, kind)] for a table's columns. kind: sum | min | max."""
    terms = [("rows", "count(*)", "count(*)", "sum")]
    if ts:
        t = qident(ts)
        div = " / 1000" if base_type(ts_type) == "TIMESTAMP_NS" else ""
        terms.append(("ts_min", "cast(min(%s) AS LONG)%s" % (t, div), "epoch_us(min(%s))" % t, "min"))
        terms.append(("ts_max", "cast(max(%s) AS LONG)%s" % (t, div), "epoch_us(max(%s))" % t, "max"))
    for c, typ in columns:
        b = base_type(typ)
        q = qident(c)
        if b == "BOOLEAN":
            e = "sum(CASE WHEN %s THEN 1 ELSE 0 END)" % q
            terms.append((c + ":true", e, e, "sum"))
            continue
        if b in SCALAR_FP_TYPES or b in ("UUID", "IPV4", "GEOHASH", "LONG256"):
            terms.append((c + ":n", "count(%s)" % q, "count(%s)" % q, "sum"))
        if b == "BINARY":
            terms.append((c + ":n", "sum(CASE WHEN length(%s) >= 0 THEN 1 ELSE 0 END)" % q, "count(%s)" % q, "sum"))
        if b not in SCALAR_FP_TYPES:
            continue  # UUID, IPv4, GEOHASH, LONG256, BINARY: non-null count only; arrays and others: not covered
        if b in ("BYTE", "SHORT", "INT"):
            terms.append((c + ":sum", "sum(cast(%s AS LONG))" % q, "sum(cast(%s AS BIGINT))" % q, "sum"))
        elif b == "LONG":
            terms.append((c + ":sum", "sum(%s %% %d)" % (q, P), "sum(%s %% %d)" % (q, P), "sum"))
        elif b == "TIMESTAMP":
            terms.append((c + ":sum", "sum(cast(%s AS LONG) %% %d)" % (q, P), "sum(epoch_us(%s) %% %d)" % (q, P), "sum"))
        elif b == "TIMESTAMP_NS":
            terms.append((c + ":sum", "sum((cast(%s AS LONG) / 1000) %% %d)" % (q, P),
                          "sum(epoch_us(%s) %% %d)" % (q, P), "sum"))
        elif b == "DATE":
            terms.append((c + ":sum", "sum(cast(cast(%s AS TIMESTAMP) AS LONG) %% %d)" % (q, P),
                          "sum(epoch_us(cast(%s AS TIMESTAMP)) %% %d)" % (q, P), "sum"))
        elif b in ("FLOAT", "DOUBLE"):
            qe = ("sum(CASE WHEN abs(cast({c} AS DOUBLE)) < 1e12 THEN cast(floor(cast({c} AS DOUBLE) * 1000000) AS LONG)"
                  " % {p} ELSE 0 END)").format(c=q, p=P)
            de = ("sum(CASE WHEN abs(cast({c} AS DOUBLE)) < 1e12 THEN cast(floor(cast({c} AS DOUBLE) * 1000000) AS BIGINT)"
                  " % {p} ELSE 0 END)").format(c=q, p=P)
            terms.append((c + ":sum", qe, de, "sum"))
        elif b in ("STRING", "VARCHAR", "SYMBOL"):
            # UTF-8 byte length on both sides, so non-ASCII text (e.g. "Bodø") compares exactly:
            # QuestDB length_bytes(varchar), DuckDB strlen. QuestDB returns -1 for NULL, hence the CASE.
            qe = "sum(CASE WHEN %s IS NULL THEN 0 ELSE length_bytes(cast(%s AS VARCHAR)) END)" % (q, q)
            de = "sum(CASE WHEN %s IS NULL THEN 0 ELSE strlen(%s) END)" % (q, q)
            terms.append((c + ":len", qe, de, "sum"))
    return terms


def fp_from_row(terms, row):
    out = {}
    for (name, _q, _d, kind), v in zip(terms, row):
        v = to_int(v)
        if v is None and kind == "sum":
            v = 0
        out[name] = v
    return out


def fp_add(a, b):
    if a is None:
        return dict(b)
    out = dict(a)
    for k, v in b.items():
        if k not in out or out[k] is None:
            out[k] = v
        elif v is None:
            pass
        elif k == "ts_min":
            out[k] = min(out[k], v)
        elif k == "ts_max":
            out[k] = max(out[k], v)
        else:
            out[k] = out[k] + v
    return out


def fp_sum(fps):
    out = None
    for f in fps:
        out = fp_add(out, f)
    return out


def fp_diff(a, b):
    """Names of the terms that differ (terms missing on either side are ignored)."""
    if a is None or b is None:
        return ["<missing>"]
    return sorted(k for k in set(a) & set(b) if to_int(a[k]) != to_int(b[k]))


def fp_verdict(a, b):
    """'ok', 'inspect' (only string-length terms differ) or 'diff'."""
    d = fp_diff(a, b)
    if not d:
        return "ok", d
    if all(k.endswith(":len") for k in d):
        return "inspect", d
    return "diff", d


def chunk_terms(e, types, ts):
    """Fingerprint terms for a stored file: from the columns recorded when it was written (a column added to the
    table later is not in older files)."""
    cols = e.get("columns") or types
    return fp_terms(cols, ts, dict((c, t) for c, t in cols).get(ts, "TIMESTAMP"))


def qdb_fp_day(qdb, table, ts, terms, day):
    sql = "SELECT %s FROM %s WHERE %s IN %s" % (", ".join(t[1] for t in terms), qident(table), qident(ts), qlit(day))
    return fp_from_row(terms, qdb.rows(sql)[0])


def qdb_fp_whole(qdb, source_sql, terms):
    sql = "SELECT %s FROM %s" % (", ".join(t[1] for t in terms), source_sql)
    return fp_from_row(terms, qdb.rows(sql)[0])


def qdb_fp_by_day(qdb, source_sql, ts, terms, period):
    """Per-day fingerprints in one grouped query (used on a restore target, never on the source server)."""
    sql = "SELECT timestamp_floor('d', %s) qb__day, %s FROM %s WHERE %s IN %s ORDER BY qb__day" % (
        qident(ts), ", ".join(t[1] for t in terms), source_sql, qident(ts), qlit(period))
    out = {}
    for r in qdb.rows(sql, timeout=3600):
        out[day_of(parse_qts(r[0]))] = fp_from_row(terms, r[1:])
    return out


def duck_fp_by_day(duck, files, ts, terms, where=""):
    sql = "SELECT epoch_us(%s) // %d AS qb__day, %s FROM read_parquet(%s) %s GROUP BY qb__day ORDER BY qb__day" % (
        qident(ts), DAY_US, ", ".join("%s AS qb__t%d" % (t[2], i) for i, t in enumerate(terms)), Duck.file_list(files), where)
    out = {}
    for r in duck.query(sql):
        out[day_from_number(to_int(r["qb__day"]))] = fp_from_row(terms, [r["qb__t%d" % i] for i in range(len(terms))])
    return out


def duck_fp_whole(duck, files, terms):
    sql = "SELECT %s FROM read_parquet(%s)" % (", ".join("%s AS qb__t%d" % (t[2], i) for i, t in enumerate(terms)),
                                               Duck.file_list(files))
    r = duck.query(sql)[0]
    return fp_from_row(terms, [r["qb__t%d" % i] for i in range(len(terms))])


# ---------------------------------------------------------------------------------------------------------------
# Schema capture and DDL generation


def ttl_clause(value, unit):
    if not value or int(value) <= 0:
        return ""
    return " TTL %d %sS" % (int(value), str(unit).upper())


def generate_ddl(name, tmeta, cols):
    """(create_sql, dedup_sql or None). Generated from tables()/table_columns(): SHOW CREATE TABLE alone does not
    round-trip (no WAL keyword, no symbol CAPACITY, no WITH parameters, TTL only in some cases)."""
    lines = []
    for c in cols:
        s = "  %s %s" % (qident(c["column"]), c["type"])
        if base_type(c["type"]) == "SYMBOL":
            if c.get("symbolCapacity"):
                s += " CAPACITY %d" % int(c["symbolCapacity"])
            s += " CACHE" if c.get("symbolCached") else " NOCACHE"
            if c.get("indexed"):
                s += " INDEX CAPACITY %d" % int(c.get("indexBlockCapacity") or 256)
        lines.append(s)
    sql = "CREATE TABLE %s (\n%s\n)" % (qident(name), ",\n".join(lines))
    ts = tmeta.get("designatedTimestamp")
    pb = tmeta.get("partitionBy") or "NONE"
    if ts:
        sql += " TIMESTAMP(%s) PARTITION BY %s" % (qident(ts), pb)
        if pb != "NONE":
            sql += ttl_clause(tmeta.get("ttlValue"), tmeta.get("ttlUnit"))
            sql += " WAL" if tmeta.get("walEnabled") else " BYPASS WAL"
            mur, lag = tmeta.get("maxUncommittedRows"), tmeta.get("o3MaxLag")
            if (mur, lag) != (DEFAULT_MAX_UNCOMMITTED_ROWS, DEFAULT_O3_MAX_LAG_US) and mur is not None:
                sql += " WITH maxUncommittedRows=%d, o3MaxLag=%dus" % (int(mur), int(lag))
    dedup = None
    keys = [c["column"] for c in cols if c.get("upsertKey")]
    if tmeta.get("dedup") and keys:
        dedup = "ALTER TABLE %s DEDUP ENABLE UPSERT KEYS(%s)" % (qident(name), ", ".join(qident(k) for k in keys))
    return sql, dedup


def table_whitelist(tmeta):
    return {k: tmeta.get(k) for k in TABLES_WHITELIST}


def columns_whitelist(cols):
    out = []
    for c in cols:
        w = {k: c.get(k) for k in COLUMNS_WHITELIST}
        if base_type(c.get("type")) != "SYMBOL":
            w["symbolCached"] = None
            w["symbolCapacity"] = None
        if not c.get("indexed"):
            w["indexBlockCapacity"] = None
        out.append(w)
    return out


def view_order(views):
    """Views in dependency order (a view after every view whose name it mentions)."""
    names = sorted(views)
    deps = {}
    for n in names:
        sql = views[n]
        deps[n] = {m for m in names if m != n and re.search(r"(?<![A-Za-z0-9_])%s(?![A-Za-z0-9_])" % re.escape(m), sql)}
    out, seen = [], set()

    def visit(n, stack):
        if n in seen:
            return
        if n in stack:
            raise Failure("views have a dependency cycle: %s" % " -> ".join(stack + [n]))
        for d in sorted(deps[n]):
            visit(d, stack + [n])
        seen.add(n)
        out.append(n)

    for n in names:
        visit(n, [])
    return out


def capture_schema(qdb, root, cfg_tables):
    """Write schema/ and return (tables_meta {name: tables() row}, views {name: sql}, schema doc)."""
    sdir = os.path.join(root, "schema")
    all_tables = {r["table_name"]: r for r in qdb.dicts("SELECT * FROM tables()")}
    views = {r["view_name"]: r["view_sql"] for r in qdb.dicts("SELECT view_name, view_sql FROM views()")}
    build = qdb.rows("SELECT build()")[0][0]
    try:
        create_db = "\n".join(r[0] for r in qdb.rows("SHOW CREATE DATABASE"))
    except QdbError as e:
        create_db = "-- SHOW CREATE DATABASE failed: %s\n" % e
    write_text(os.path.join(sdir, "create_database.sql"), create_db + "\n")
    write_text(os.path.join(sdir, "build.txt"), build + "\n")

    doc = {"format": FORMAT, "build": build, "captured_at": iso_now(), "tables": {}, "views": []}
    # Keep only the definition fields of tables(): the rest (row counts, txn numbers, write-amp stats) change on
    # every write and are never a pass criterion.
    objects = {}
    for n, r in sorted(all_tables.items()):
        objects[n] = {"table_type": r.get("table_type"), "matView": r.get("matView")}
        objects[n].update(table_whitelist(r))
    write_json(os.path.join(sdir, "objects.json"), objects)

    for t in cfg_tables:
        name = t["table"]
        if t["mode"] == "skip" or name not in all_tables:
            continue
        tmeta = all_tables[name]
        cols = qdb.dicts("SELECT * FROM table_columns(%s)" % qlit(name))
        create, dedup = generate_ddl(name, tmeta, cols)
        try:
            show = "\n".join(r[0] for r in qdb.rows("SHOW CREATE TABLE %s" % qident(name)))
        except QdbError as e:
            show = "-- SHOW CREATE TABLE failed: %s" % e
        fn = safe_name(name)
        write_json(os.path.join(sdir, "columns", fn + ".json"), cols)
        ddl = "-- Generated from tables() and table_columns() by qdb-backup.py. Restore creates the table, loads the data,\n" \
              "-- then enables DEDUP (so rows with equal keys in the backup are all kept).\n" + create + ";\n"
        if dedup:
            ddl += dedup + ";\n"
        write_text(os.path.join(sdir, "ddl", fn + ".sql"), ddl)
        write_text(os.path.join(sdir, "ddl", fn + ".show.sql"), "-- SHOW CREATE TABLE (reference only)\n" + show + "\n")
        doc["tables"][name] = {"create": create, "dedup": dedup, "table": table_whitelist(tmeta),
                               "columns": columns_whitelist(cols),
                               "types": [[c["column"], c["type"]] for c in cols]}
    vdir = os.path.join(sdir, "views")
    if os.path.isdir(vdir):
        for f in os.listdir(vdir):
            rm_f(os.path.join(vdir, f))
    # A view is kept when every table or view it names is in the backup (restore can then create it).
    kept = {t["table"] for t in cfg_tables if t["mode"] != "skip" and t["table"] in all_tables}
    doc["views_left_out"] = {}
    i = 0
    for v in view_order(views):
        refs = {n for n in all_tables if n != v and re.search(
            r"(?<![A-Za-z0-9_])%s(?![A-Za-z0-9_])" % re.escape(n), views[v])}
        missing = sorted(refs - kept)
        if missing:
            doc["views_left_out"][v] = missing
            continue
        kept.add(v)
        i += 1
        sql = "CREATE VIEW %s AS (%s)" % (qident(v), views[v].strip())
        write_text(os.path.join(vdir, "%02d_%s.sql" % (i, safe_name(v))), sql + ";\n")
        doc["views"].append({"name": v, "sql": sql})
    write_json(os.path.join(sdir, "tables.json"), doc)
    return all_tables, views, doc


# ---------------------------------------------------------------------------------------------------------------
# Config


def load_config(path):
    cfg = read_json(path)
    if cfg is None:
        raise SystemExit("config not found: %s" % path)
    if not isinstance(cfg.get("tables"), list):
        raise SystemExit("config: 'tables' must be a list")
    d = cfg.get("defaults", {})
    out = []
    seen = set()
    for t in cfg["tables"]:
        t = dict(t)
        if "table" not in t:
            raise SystemExit("config: every entry needs 'table'")
        if t["table"] in seen:
            raise SystemExit("config: %s listed twice" % t["table"])
        seen.add(t["table"])
        t.setdefault("mode", d.get("mode", "chunked"))
        if t["mode"] not in ("chunked", "full", "schema", "skip"):
            raise SystemExit("config: %s: mode must be chunked|full|schema|skip" % t["table"])
        t.setdefault("prefix", t["table"])
        t.setdefault("dir", d.get("dir", "."))
        t.setdefault("settle_days", d.get("settle_days", 1))
        t.setdefault("timeout", d.get("timeout", 280))
        if t.pop("fetch", "month") != "month" or d.get("fetch", "month") != "month":
            raise SystemExit("config: %s: 'fetch' was removed: every month is one request (raise 'timeout' if a "
                             "month does not fit)" % t["table"])
        start = t.get("start", d.get("start"))
        if start is not None:
            if not re.match(r"^\d{4}-(0[1-9]|1[0-2])$", str(start)):
                raise SystemExit("config: %s: start must be YYYY-MM" % t["table"])
            t["start"] = str(start)
        if os.path.isabs(t["dir"]) or ".." in t["dir"].split("/"):
            raise SystemExit("config: %s: dir must be relative to the backup root" % t["table"])
        out.append(t)
    cfg["tables"] = out
    cfg.setdefault("unlisted", "error")
    return cfg


# ---------------------------------------------------------------------------------------------------------------
# Lock, preflight


class Lock:
    def __init__(self, root):
        self.path = os.path.join(root, ".qdb-backup.lock")
        self.f = None

    def __enter__(self):
        self.f = open(self.path, "a+")
        if fcntl is not None:
            try:
                fcntl.flock(self.f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                raise SystemExit("another qdb-backup.py is working on %s (lock %s)" % (os.path.dirname(self.path), self.path))
        self.f.seek(0)
        self.f.truncate()
        self.f.write("%d %s\n" % (os.getpid(), iso_now()))
        self.f.flush()
        return self

    def __exit__(self, *a):
        if self.f:
            self.f.close()


def check_clock(qdb, max_skew_s):
    t0 = time.time()
    r = qdb.rows("SELECT now()")[0][0]
    t1 = time.time()
    skew = parse_qts(r) / 1e6 - (t0 + t1) / 2
    if abs(skew) > max_skew_s:
        raise SystemExit("clock check: this machine and QuestDB differ by %.0f s (limit %d s). The settle rule reads "
                         "this clock; fix the time (NTP) or pass --max-clock-skew." % (skew, max_skew_s))
    return skew


def largest_file(root):
    best = 0
    for dp, _dn, fn in os.walk(root):
        for f in fn:
            if f.endswith(".parquet"):
                try:
                    best = max(best, os.path.getsize(os.path.join(dp, f)))
                except OSError:
                    pass
    return best


# ---------------------------------------------------------------------------------------------------------------
# Partition metadata (tier 1: no table scan)


def read_partitions(qdb, table):
    rows = qdb.dicts("SELECT name, minTimestamp, maxTimestamp, numRows, seqTxn FROM table_partitions(%s)" % qlit(table))
    out = []
    for i, r in enumerate(rows):
        n = to_int(r.get("numRows")) or 0
        if n <= 0 or r.get("minTimestamp") is None:
            continue
        out.append({"i": i, "name": r["name"], "min": parse_qts(r["minTimestamp"]), "max": parse_qts(r["maxTimestamp"]),
                    "rows": n, "seq": to_int(r.get("seqTxn"))})
    return out


def _meta_of(parts):
    if not parts:
        return None
    seqs = [p["seq"] for p in parts if p["seq"] is not None and p["seq"] >= 0]
    return {"rows": sum(p["rows"] for p in parts), "ts_min": us_to_iso(min(p["min"] for p in parts)),
            "ts_max": us_to_iso(max(p["max"] for p in parts)), "seq_max": max(seqs) if seqs else None,
            "partitions": len(parts)}


def build_meta(parts):
    """({month: meta}, {day: meta}) from the partitions overlapping each period. Partitions are grouped by their
    time range, never by name (a day can be split into several partitions, e.g. 2026-11-07T105622-000001)."""
    by_day, by_month = {}, {}
    for p in parts:
        for d in day_range(day_of(p["min"]), day_of(p["max"])):
            by_day.setdefault(d, {})[p["i"]] = p
            by_month.setdefault(d[:7], {})[p["i"]] = p
    days = {d: _meta_of(list(v.values())) for d, v in by_day.items()}
    months = {m: _meta_of(list(v.values())) for m, v in by_month.items()}
    return months, days


# ---------------------------------------------------------------------------------------------------------------
# One table


class TableJob:
    def __init__(self, ctx, tcfg, tmeta, types):
        self.ctx = ctx
        self.qdb = ctx.qdb
        self.duck = ctx.duck
        self.cfg = tcfg
        self.name = tcfg["table"]
        self.prefix = tcfg["prefix"]
        self.dir = tcfg["dir"]
        self.tmeta = tmeta
        self.types = types  # [[col, type]]
        self.ts = tmeta.get("designatedTimestamp")
        ts_type = dict((c, t) for c, t in types).get(self.ts, "TIMESTAMP")
        self.terms = fp_terms(types, self.ts, ts_type)
        self.settle_us = int(tcfg["settle_days"]) * DAY_US
        self.exact_meta = tmeta.get("partitionBy") in ("DAY", "HOUR")
        self.mpath = os.path.join(self.dir, self.prefix + ".manifest.json")
        self.man = read_json(self.abs(self.mpath)) or {}
        self.man.setdefault("format", FORMAT)
        self.man.update({"table": self.name, "prefix": self.prefix, "dir": self.dir, "mode": tcfg["mode"],
                         "ts": self.ts, "partition_by": tmeta.get("partitionBy"), "start": tcfg.get("start")})
        self.man.pop("fetch", None)
        self.man.setdefault("chunks", {})
        self.man.setdefault("years", {})
        self.stats = {"exported": 0, "skipped": 0, "meta_only": 0, "adopted": 0, "failed": 0, "bytes": 0, "notes": []}

    # paths are relative to the backup root in the manifest
    def abs(self, rel):
        return os.path.join(self.ctx.root, rel)

    def rel(self, *parts):
        return os.path.normpath(os.path.join(self.dir, *parts))

    def month_rel(self, m):
        return self.rel(self.prefix + "_parquet", "%s_%s.parquet" % (self.prefix, m))

    def year_rel(self, y):
        return self.rel("%s_%s.parquet" % (self.prefix, y))

    def full_rel(self):
        return self.rel(self.prefix + ".parquet")

    def save(self):
        if self.ctx.args.dry_run:
            return
        self.man["updated_at"] = iso_now()
        write_json(self.abs(self.mpath), self.man)

    def note(self, msg):
        self.stats["notes"].append(msg)
        log("    note: " + msg)

    def settled(self, end_us):
        return now_us() >= end_us + self.settle_us

    def src(self):
        return qident(self.name)

    def source_fp_day(self, day):
        return qdb_fp_day(self.qdb, self.name, self.ts, self.terms, day)

    def record_file(self, entry, rel):
        p = self.abs(rel)
        entry["file"] = rel
        entry["bytes"] = os.path.getsize(p)
        entry["sha256"] = sha256_file(p)

    # ---- full mode -------------------------------------------------------------------------------------------

    def run_full(self):
        rel = self.full_rel()
        part = self.abs(rel) + ".part"
        os.makedirs(os.path.dirname(part), exist_ok=True)
        cnt = to_int(self.qdb.rows("SELECT count() FROM %s" % self.src())[0][0])
        if cnt > self.ctx.args.full_max_rows:
            raise Failure("%d rows is too many for mode 'full' (limit %d); use 'chunked'" % (cnt, self.ctx.args.full_max_rows))
        if self.ctx.args.dry_run:
            log("  %-10s %s (%d rows)" % ("dump", rel, cnt))
            return
        if self.tmeta.get("walEnabled"):
            self.qdb.wait_wal(self.name)
        fp1 = qdb_fp_whole(self.qdb, self.src(), self.terms)
        n, secs = self.qdb.download("SELECT * FROM %s" % self.src(), part, self.cfg["timeout"])
        fp2 = qdb_fp_whole(self.qdb, self.src(), self.terms)
        if fp1 != fp2:
            rm_f(part)
            raise Failure("the table changed during the dump; run again")
        checked = False
        if self.duck:
            v, d = fp_verdict(fp1, duck_fp_whole(self.duck, [part], self.terms))
            if v == "diff":
                rm_f(part)
                raise Failure("file fingerprint differs from the table: %s" % ", ".join(d))
            if v == "inspect":
                self.note("string lengths differ between QuestDB and DuckDB (non-ASCII text?): %s" % ", ".join(d))
            checked = True
        old = self.man.get("full") or {}
        if old.get("fp") == fp1 and os.path.exists(self.abs(rel)) and old.get("sha256"):
            rm_f(part)
            old["checked_at"] = iso_now()
            self.man["full"] = old
            self.stats["skipped"] += 1
            log("  %-10s %s (%d rows, unchanged)" % ("same", rel, fp1["rows"]))
        else:
            os.replace(part, self.abs(rel))
            entry = {"exported_at": iso_now(), "checked_at": iso_now(), "fp": fp1, "rows": fp1["rows"],
                     "file_fp_checked": checked, "columns": self.types}
            self.record_file(entry, rel)
            self.man["full"] = entry
            self.stats["exported"] += 1
            self.stats["bytes"] += entry["bytes"]
            log("  %-10s %s (%d rows, %s in %.1fs)" % ("dumped", rel, fp1["rows"], human_bytes(n), secs))
        self.save()

    # ---- chunked mode ----------------------------------------------------------------------------------------

    def run_chunked(self):
        if not self.ts:
            raise Failure("mode 'chunked' needs a designated timestamp; use 'full'")
        parts = read_partitions(self.qdb, self.name)
        mmeta, dmeta = build_meta(parts)
        self.adopt_years(mmeta, dmeta)
        start = self.cfg.get("start")
        if start:
            # Months before 'start' are kept elsewhere: never exported, and left as they are if a file of theirs
            # was adopted.
            before = sorted(m for m in mmeta if m < start and m not in self.man["chunks"])
            if before:
                log("  %-10s %s..%s  (%d month(s) before start %s: kept elsewhere, not backed up here)"
                    % ("ignore", before[0], before[-1], len(before), start))
            mmeta = {m: v for m, v in mmeta.items() if m >= start}
            dmeta = {d: v for d, v in dmeta.items() if d[:7] >= start}
        months = sorted(set(mmeta) | {m for m in self.man["chunks"] if not start or m >= start})
        force = set(self.ctx.args.refetch or [])
        self.guard_new(months, mmeta, force)
        for m in months:
            try:
                self.process_month(m, mmeta.get(m), {d: v for d, v in dmeta.items() if d[:7] == m}, m in force)
            except (Failure, QdbError) as e:
                self.stats["failed"] += 1
                log("  %-10s %s  %s" % ("FAILED", m, e))
                self.ctx.failures.append("%s %s: %s" % (self.name, m, e))
            self.save()

    def guard_new(self, months, mmeta, force):
        """Stop before downloading anything when many settled months would be exported although no file of theirs
        exists: an empty or wrong backup root, or old data that is kept elsewhere (set 'start' in the config)."""
        stale = set()
        for yr in self.man["years"].values():
            stale.update(yr.get("stale_months") or [])
        new = [m for m in months if mmeta.get(m) is not None and m not in force
               and self.man["chunks"].get(m) is None and self.settled(month_end_us(m))
               and not is_parquet(self.abs(self.month_rel(m)))]
        limit = self.ctx.args.max_new
        if len(new) <= limit:
            return
        rows = sum(mmeta[m]["rows"] for m in new)
        n_stale = len([m for m in new if m in stale])
        msg = ("%d settled month(s) have no current file in this backup and would be exported from QuestDB (%s..%s, "
               "%d rows%s), more than --max-new %d. Is BACKUP_ROOT right, with the existing files copied in? Months "
               "kept elsewhere: set 'start' in the config. A first backup: pass --max-new %d."
               % (len(new), new[0], new[-1], rows,
                  ("; %d of them differ from the yearly file" % n_stale) if n_stale else "", limit, len(new)))
        if self.ctx.args.dry_run:
            self.note("a real run would stop here: " + msg)
            return
        raise Failure(msg)

    def adopt_years(self, mmeta, dmeta):
        """Existing <prefix>_<YYYY>.parquet files with no manifest entry: check each month against the partition
        metadata (one local DuckDB pass, nothing on the server) and record the matching months."""
        ydir = self.abs(self.dir)
        if not os.path.isdir(ydir):
            return
        if self.duck:
            # Years adopted earlier without DuckDB (file time only): check them now.
            for y in sorted(self.man["years"]):
                unchecked = [m for m, c in self.man["chunks"].items() if c.get("year") == y and c.get("verified") == "none"]
                if unchecked:
                    log("  %-10s %s (adopted without DuckDB: checking it now)" % ("recheck", self.year_rel(y)))
                    for m in unchecked:
                        del self.man["chunks"][m]
                    del self.man["years"][y]
        pat = re.compile(r"^%s_(\d{4})\.parquet$" % re.escape(self.prefix))
        for f in sorted(os.listdir(ydir)):
            mt = pat.match(f)
            if not mt or mt.group(1) in self.man["years"]:
                continue
            y = mt.group(1)
            rel = self.year_rel(y)
            if not is_parquet(self.abs(rel)):
                self.note("%s is not complete Parquet; ignored" % rel)
                continue
            log("  %-10s %s (%s)" % ("adopt", rel, "checking each month against the source metadata" if self.duck
                                      else "no DuckDB: taken as it is, checked on the next run with DuckDB"))
            months_ok, stale = [], []
            if self.duck:
                summ = self.duck_summary([self.abs(rel)])
                for m in sorted(summ):
                    if self.man["chunks"].get(m):
                        continue
                    if os.path.exists(self.abs(self.month_rel(m))):
                        continue  # the monthly file is adopted on its own
                    if mmeta.get(m) is None or self.meta_matches(summ[m], mmeta.get(m)):
                        months_ok.append(m)  # (no rows in the source any more: the year file is the only copy)
                    else:
                        stale.append(m)
                verified = "meta"
            else:
                months_ok = [m for m in sorted(mmeta) if m[:4] == y and not self.man["chunks"].get(m)
                             and not os.path.exists(self.abs(self.month_rel(m)))]
                verified = "none"
            ent = {"adopted_at": iso_now(), "months": months_ok, "stale_months": stale}
            self.record_file(ent, rel)
            self.man["years"][y] = ent
            for m in months_ok:
                self.man["chunks"][m] = {"file": rel, "year": y, "adopted_at": iso_now(), "settled": True,
                                         "stable": True, "verified": verified, "meta": mmeta.get(m),
                                         "meta_days": {d: v for d, v in dmeta.items() if d[:7] == m},
                                         "fp_days": None, "columns": self.types}
                if mmeta.get(m) is None:
                    self.man["chunks"][m]["source_gone"] = True
            self.stats["adopted"] += len(months_ok)
            log("             %d month(s) adopted (%s)%s" % (len(months_ok), verified,
                ("; differ from the source, re-exported as monthly files: " + ", ".join(stale)) if stale else ""))
            self.save()

    def duck_summary(self, files):
        """{month: {rows, ts_min, ts_max}} of files, via DuckDB (UTC, epoch_us)."""
        sql = ("SELECT epoch_us({t}) // {day} AS qb__day, count(*) AS qb__n, epoch_us(min({t})) AS qb__lo, "
               "epoch_us(max({t})) AS qb__hi FROM read_parquet({f}) GROUP BY qb__day").format(t=qident(self.ts), day=DAY_US, f=Duck.file_list(files))
        out = {}
        for r in self.duck.query(sql):
            m = day_from_number(to_int(r["qb__day"]))[:7]
            x = out.setdefault(m, {"rows": 0, "lo": None, "hi": None})
            x["rows"] += to_int(r["qb__n"])
            lo, hi = to_int(r["qb__lo"]), to_int(r["qb__hi"])
            x["lo"] = lo if x["lo"] is None else min(x["lo"], lo)
            x["hi"] = hi if x["hi"] is None else max(x["hi"], hi)
        return out

    def meta_matches(self, s, meta):
        return (self.exact_meta and meta is not None and s["rows"] == meta["rows"]
                and s["lo"] == parse_qts(meta["ts_min"]) and s["hi"] == parse_qts(meta["ts_max"]))

    def process_month(self, m, meta, mdays, forced):
        e = self.man["chunks"].get(m)
        settled = self.settled(month_end_us(m))
        rel = self.month_rel(m)
        if meta is None:
            if e and not e.get("source_gone"):
                e["source_gone"] = True
                self.note("%s: no rows in the source any more; the backup keeps its copy" % m)
            self.stats["skipped"] += 1
            return
        if e and e.get("source_gone"):
            e.pop("source_gone", None)
        if forced:
            return self.export_month(m, "forced", meta)
        if e is None:
            if settled and is_parquet(self.abs(rel)):
                return self.adopt_month(m, meta, mdays)
            if not settled:
                return self.export_month(m, "open", meta)
            in_year = any(m in (yr.get("stale_months") or []) for yr in self.man["years"].values())
            return self.export_month(m, "stale: differs from the yearly file" if in_year else "new", meta)
        if not settled:
            return self.export_month(m, "open", meta)
        if not e.get("settled"):
            return self.export_month(m, "final", meta)
        if e.get("stable") is False:
            return self.export_month(m, "unstable", meta)
        if not os.path.exists(self.abs(e["file"])):
            return self.export_month(m, "missing", meta)
        if e.get("bytes") is not None and os.path.getsize(self.abs(e["file"])) != e["bytes"]:
            return self.export_month(m, "damaged: size differs from the manifest", meta)
        if self.duck and e.get("adopted_at") and e.get("verified") == "none" and not e.get("year"):
            log("  %-10s %s  (adopted without DuckDB: checking it now)" % ("recheck", m))
            return self.adopt_month(m, meta, mdays)
        if e.get("meta") == meta:
            self.stats["skipped"] += 1
            if self.ctx.args.verbose:
                log("  %-10s %s" % ("skip", m))
            return
        reason = self.meta_change(e.get("meta"), meta)
        if self.same_content(e, mdays):
            e["meta"], e["meta_days"] = meta, mdays
            self.stats["meta_only"] += 1
            log("  %-10s %s  (%s; the content of the changed days is the same: nothing to download)" % ("same", m, reason))
            return
        return self.export_month(m, "changed: " + reason, meta)

    @staticmethod
    def meta_change(old, new):
        if not old:
            return "no metadata"
        bits = []
        if old.get("rows") != new.get("rows"):
            bits.append("rows %+d" % ((new.get("rows") or 0) - (old.get("rows") or 0)))
        if old.get("seq_max") != new.get("seq_max"):
            bits.append("written to")
        if old.get("ts_min") != new.get("ts_min") or old.get("ts_max") != new.get("ts_max"):
            bits.append("time range")
        if old.get("partitions") != new.get("partitions"):
            bits.append("partitions %s->%s" % (old.get("partitions"), new.get("partitions")))
        return ", ".join(bits) or "metadata"

    def same_content(self, e, mdays):
        """Tier 2 on the changed days only: if their content fingerprint is unchanged, there is nothing to export
        (e.g. a reload that rewrote identical rows)."""
        fpd = e.get("fp_days")
        old_days = e.get("meta_days") or {}
        if not fpd or not self.exact_meta:
            return False
        changed = [d for d in sorted(set(mdays) | set(old_days)) if mdays.get(d) != old_days.get(d)]
        if not changed or len(changed) > 10:
            return False
        for d in changed:
            if d not in mdays or d not in fpd:
                return False
            if self.ctx.args.dry_run:
                return False
            if fp_diff(self.source_fp_day(d), fpd[d]):
                return False
        return True

    def adopt_month(self, m, meta, mdays):
        rel = self.month_rel(m)
        p = self.abs(rel)
        if self.duck:
            s = self.duck_summary([p]).get(m)
            if s is None or not self.meta_matches(s, meta):
                return self.export_month(m, "redo: the file differs from the source", meta)
            verified = "meta"
        else:
            # Without DuckDB: the old qdb-export.sh rule (a file written after its period ended + settle is final).
            if os.path.getmtime(p) * 1e6 < month_end_us(m) + self.settle_us:
                return self.export_month(m, "redo: written before the month settled", meta)
            verified = "none"
        if self.ctx.args.dry_run:
            log("  %-10s %s  (%s)" % ("adopt", m, verified))
            return
        e = {"adopted_at": iso_now(), "settled": True, "stable": True, "verified": verified, "meta": meta,
             "meta_days": mdays, "fp_days": None, "columns": self.types}
        self.record_file(e, rel)
        self.man["chunks"][m] = e
        self.stats["adopted"] += 1
        log("  %-10s %s  (%s, verified by %s)" % ("adopted", m, human_bytes(e["bytes"]),
                                                  "metadata" if verified == "meta" else "file time only"))

    def export_month(self, m, reason, meta_hint):
        rel = self.month_rel(m)
        if self.ctx.args.dry_run:
            log("  %-10s %s  (%s, ~%s rows)" % ("export", m, reason, meta_hint.get("rows")))
            return
        e_old = self.man["chunks"].get(m) or {}
        settled = self.settled(month_end_us(m))
        if self.tmeta.get("walEnabled") and not self.qdb.wait_wal(self.name):
            self.note("%s: WAL not applied after 60 s; exporting anyway (the stable check decides)" % m)
        mm1, md1 = build_meta(read_partitions(self.qdb, self.name))
        meta1 = mm1.get(m)
        if meta1 is None:
            raise Failure("no rows any more")
        p = self.abs(rel)
        part = p + ".part"
        os.makedirs(os.path.dirname(p), exist_ok=True)
        rm_f(part)
        sql = "SELECT * FROM %s WHERE %s IN %s" % (self.src(), qident(self.ts), qlit(m))
        n, secs = self.qdb.download(sql, part, self.cfg["timeout"])
        try:
            fp_days, stable, checked = None, None, False
            if settled:
                days = sorted(d for d in md1 if d[:7] == m)
                fp_days = {d: self.source_fp_day(d) for d in days}
                mm2, _ = build_meta(read_partitions(self.qdb, self.name))
                stable = mm2.get(m) == meta1
                if not stable:
                    # Written to during the export: the file is kept (it is no older than the last one) but the
                    # fingerprints describe a later state, so none is recorded and the next run exports it again.
                    fp_days = None
            if self.duck and settled and stable:
                self.verify_file(part, fp_days, m)
                checked = True
            new_rows = sum(f["rows"] for f in fp_days.values()) if fp_days else None
            old_rows = (e_old.get("meta") or {}).get("rows")
            if (settled and e_old.get("settled") and new_rows is not None and old_rows
                    and new_rows < 0.9 * old_rows and not self.ctx.args.accept_shrink):
                raise Failure("rows fell from %d to %d (>10%%): a reload in progress? Kept the old file; rerun later "
                              "or pass --accept-shrink" % (old_rows, new_rows))
        except BaseException:
            rm_f(part)
            raise
        os.replace(part, p)
        e = {"exported_at": iso_now(), "settled": settled, "stable": stable, "verified": "fp" if fp_days else "none",
             "file_fp_checked": checked, "meta": meta1, "meta_days": {d: v for d, v in md1.items() if d[:7] == m},
             "fp_days": fp_days, "columns": self.types}
        self.record_file(e, rel)
        y = m[:4]
        if e_old.get("year") or (y in self.man["years"] and m in self.man["years"][y].get("months", [])):
            yr = self.man["years"].get(y)
            if yr is not None and m not in yr.setdefault("stale_months", []):
                yr["stale_months"].append(m)
                yr["stale_months"].sort()
        self.man["chunks"][m] = e
        self.stats["exported"] += 1
        self.stats["bytes"] += e["bytes"]
        status = reason.split(":")[0]
        extra = "" if stable is not False else "  UNSTABLE (written during export; re-exported next run)"
        log("  %-10s %s  %10s in %5.1fs  %s%s" % (status, m, human_bytes(n), secs,
                                                    ("(%s)" % reason) if ":" in reason else "", extra))

    def verify_file(self, path, fp_days, label):
        """Compare a file's per-day DuckDB fingerprint with QuestDB's. Raises Failure on a real difference."""
        ffp = duck_fp_by_day(self.duck, [path], self.ts, self.terms)
        problems, inspect = [], []
        for d in sorted(set(ffp) | set(fp_days)):
            v, diff = fp_verdict(fp_days.get(d), ffp.get(d))
            if v == "diff":
                problems.append("%s: %s" % (d, ", ".join(diff[:6])))
            elif v == "inspect":
                inspect.append(d)
        if problems:
            raise Failure("%s: file fingerprint differs from QuestDB: %s" % (label, "; ".join(problems[:5])))
        if inspect:
            self.note("%s: only string lengths differ between QuestDB and DuckDB on %d day(s) (non-ASCII text?)"
                      % (label, len(inspect)))

    # ---- deep check ------------------------------------------------------------------------------------------

    def deep_check(self, limit):
        """Tier 2 for settled months: source fingerprint per day (QuestDB) against the manifest and the file
        (DuckDB). A month that differs is marked unstable, so the next run re-exports it."""
        done = 0
        chunks = self.man["chunks"]
        order = sorted((c.get("deep_checked_at") or "", m) for m, c in chunks.items() if c.get("settled"))
        for _, m in order:
            if done >= limit:
                break
            e = chunks[m]
            if e.get("source_gone"):
                continue
            days = sorted(e.get("meta_days") or {})
            src = {d: self.source_fp_day(d) for d in days}
            verdict = "ok"
            if e.get("fp_days"):
                for d in days:
                    if fp_diff(src[d], e["fp_days"].get(d)):
                        verdict = "differs from manifest on %s" % d
                        break
            if verdict == "ok" and self.duck and os.path.exists(self.abs(e["file"])):
                where = "WHERE epoch_us(%s) >= %d AND epoch_us(%s) < %d" % (
                        qident(self.ts), month_start_us(m), qident(self.ts), month_end_us(m))
                ffp = duck_fp_by_day(self.duck, [self.abs(e["file"])], self.ts, chunk_terms(e, self.types, self.ts), where)
                for d in sorted(set(days) | set(ffp)):
                    v, _ = fp_verdict(src.get(d), ffp.get(d))
                    if v == "diff":
                        verdict = "file differs from source on %s" % d
                        break
                if verdict == "ok" and not e.get("fp_days"):
                    e["fp_days"] = src
                    e["verified"] = "fp"
            e["deep_checked_at"] = iso_now()
            if verdict != "ok":
                e["stable"] = False
                self.ctx.failures.append("%s %s: deep check: %s (re-exported next run)" % (self.name, m, verdict))
            log("  %-10s %s  %s" % ("deep", m, verdict))
            done += 1
            self.save()


# ---------------------------------------------------------------------------------------------------------------
# run


class Ctx:
    pass


def write_sums(root):
    """SHA256SUMS from the sha256 already in the manifests (no re-hashing of data files), plus the small schema and
    manifest files hashed fresh. `sha256sum -c SHA256SUMS` (Linux) or `shasum -a 256 -c SHA256SUMS` (macOS)."""
    lines = {}
    bk = read_json(os.path.join(root, "BACKUP.json"), {})
    for t in bk.get("tables", []):
        mp = t.get("manifest")
        if not mp or not os.path.exists(os.path.join(root, mp)):
            continue
        man = read_json(os.path.join(root, mp))
        files = []
        if man.get("full"):
            files.append(man["full"])
        files += list(man.get("chunks", {}).values())
        files += list(man.get("years", {}).values())
        for e in files:
            if e.get("file") and e.get("sha256"):
                lines[e["file"]] = e["sha256"]
        lines[mp] = sha256_file(os.path.join(root, mp))
    sdir = os.path.join(root, "schema")
    for dp, _dn, fn in os.walk(sdir):
        for f in fn:
            if f.endswith(".tmp"):
                continue
            p = os.path.join(dp, f)
            lines[os.path.relpath(p, root)] = sha256_file(p)
    write_text(os.path.join(root, "SHA256SUMS"), "".join("%s  %s\n" % (h, f) for f, h in sorted(lines.items())))
    return len(lines)


def cmd_run(args):
    cfg = load_config(args.config)
    if args.timeout:
        for t in cfg["tables"]:
            t["timeout"] = args.timeout
    root = os.path.abspath(args.root)
    os.makedirs(root, exist_ok=True)
    with Lock(root):
        qdb = Qdb(args.url)
        ctx = Ctx()
        ctx.args, ctx.root, ctx.qdb, ctx.failures = args, root, qdb, []
        ctx.duck = None if args.no_duckdb else Duck.find(args.duckdb, temp_dir=os.path.join(root, ".duckdb-tmp"),
                                                         memory_limit=args.duckdb_memory)
        if ctx.duck is None and not args.no_duckdb:
            raise SystemExit("run needs the DuckDB CLI (--duckdb PATH or DUCKDB=PATH): it checks every file against "
                             "QuestDB before the backup trusts it. --no-duckdb runs without, unchecked.")
        log("qdb-backup run  %s -> %s" % (qdb.url, root))
        skew = check_clock(qdb, args.max_clock_skew)
        log("  QuestDB %s; clock skew %.1f s; DuckDB: %s" % (
            qdb.rows("SELECT build()")[0][0].replace("Build Information: ", "").split(",")[0], skew,
            ctx.duck.exe if ctx.duck else "not used (--no-duckdb: files are not checked locally)"))
        free = shutil.disk_usage(root).free
        need = max(2 * largest_file(root), int(args.min_free_gb * 1024 ** 3))
        if free < need and not args.dry_run:
            raise SystemExit("not enough free disk at %s: %s free, need %s (2 x the largest file, at least "
                             "--min-free-gb)" % (root, human_bytes(free), human_bytes(need)))
        all_tables, views, schema = capture_schema(qdb, root, cfg["tables"])
        for v, miss in sorted(schema.get("views_left_out", {}).items()):
            log("  view %s is not in the backup: it uses %s, which the config does not back up" % (v, ", ".join(miss)))
        listed = {t["table"] for t in cfg["tables"]}
        unlisted = sorted(n for n, r in all_tables.items() if r.get("table_type") != "V" and n not in listed)
        only = set(args.only or [])
        summary = []
        for t in cfg["tables"]:
            name = t["table"]
            if only and name not in only:
                continue
            if t["mode"] == "skip":
                continue
            if name not in all_tables:
                ctx.failures.append("%s: listed in the config but not in QuestDB" % name)
                log("\n%s: NOT FOUND in QuestDB" % name)
                continue
            log("\n%s  (%s%s, dir %s)" % (name, t["mode"], (", from " + t["start"]) if t.get("start") else "", t["dir"]))
            if t["mode"] == "schema":
                log("  schema only (schema/ddl/%s.sql)" % safe_name(name))
                summary.append({"table": name, "mode": "schema", "dir": t["dir"]})
                continue
            job = None
            try:
                job = TableJob(ctx, t, all_tables[name], schema["tables"][name]["types"])
                if t["mode"] == "full":
                    job.run_full()
                else:
                    job.run_chunked()
                    if args.deep_check and not args.dry_run:
                        job.deep_check(args.deep_limit)
            except (Failure, QdbError) as e:
                ctx.failures.append("%s: %s" % (name, e))
                log("  FAILED  %s" % e)
            if job is None:
                continue
            if not args.dry_run:
                job.save()
            summary.append({"table": name, "mode": t["mode"], "dir": t["dir"], "prefix": t["prefix"],
                            "manifest": job.mpath, "stats": job.stats})
        if args.dry_run:
            log("\n(dry run: nothing downloaded or written except schema/)")
        else:
            write_backup_json(root, cfg, schema, summary, args, qdb)
            n = write_sums(root)
            log("\nSHA256SUMS: %d files" % n)
        log("")
        for s in summary:
            st = s.get("stats")
            if st:
                log("  %-22s exported %d  skipped %d  same %d  adopted %d  failed %d  %s" % (
                    s["table"], st["exported"], st["skipped"], st["meta_only"], st["adopted"], st["failed"],
                    human_bytes(st["bytes"])))
        if ctx.failures:
            log("\nFAILED (%d):" % len(ctx.failures))
            for f in ctx.failures:
                log("  " + f)
        if unlisted:
            msg = "\nUNLISTED tables (in QuestDB, not in the config): %s\n  Add each to the config (chunked, full, " \
                  "schema or skip)." % ", ".join(unlisted)
            log(msg)
        if ctx.failures:
            return 1
        if unlisted and cfg["unlisted"] == "error":
            return 3
        return 0


def write_backup_json(root, cfg, schema, summary, args, qdb):
    path = os.path.join(root, "BACKUP.json")
    old = read_json(path, {})
    notes = dict(old.get("provenance", {}))
    for n in args.note or []:
        if "=" not in n:
            raise SystemExit("--note needs KEY=VALUE")
        k, v = n.split("=", 1)
        notes[k] = v
    tables = []
    for t in cfg["tables"]:
        if t["mode"] == "skip":
            continue
        ent = {"table": t["table"], "mode": t["mode"], "dir": t["dir"], "prefix": t["prefix"]}
        if t["mode"] in ("chunked", "full"):
            ent["manifest"] = os.path.join(t["dir"], t["prefix"] + ".manifest.json")
            man = read_json(os.path.join(root, os.path.normpath(ent["manifest"])), {})
            if man.get("full"):
                ent["rows"] = man["full"].get("rows")
                ent["bytes"] = man["full"].get("bytes")
            ch = man.get("chunks", {})
            if ch:
                ent["months"] = len(ch)
                ent["first_month"], ent["last_month"] = min(ch), max(ch)
                ent["rows"] = sum((c.get("meta") or {}).get("rows") or 0 for c in ch.values())
                ent["open_months"] = sorted(m for m, c in ch.items() if not c.get("settled"))
                ent["unstable_months"] = sorted(m for m, c in ch.items() if c.get("stable") is False)
                ent["years"] = sorted(man.get("years", {}))
            ent["manifest"] = os.path.normpath(ent["manifest"])
        tables.append(ent)
    doc = {"format": FORMAT, "tool": "qdb-backup.py", "updated_at": iso_now(),
           "source_build": schema["build"], "provenance": notes,
           "config": {"tables": cfg["tables"], "unlisted": cfg["unlisted"]},
           "tables": tables, "views": [v["name"] for v in schema["views"]],
           "schema": "schema/tables.json",
           "restore": "qdb-backup.py restore --backup <this directory> --url http://localhost:9000 "
                      "(QuestDB of the same version, with this directory as its sql copy root)"}
    write_json(path, doc)


# ---------------------------------------------------------------------------------------------------------------
# merge-year


def cmd_merge_year(args):
    root = os.path.abspath(args.root)
    bk = read_json(os.path.join(root, "BACKUP.json"))
    if not bk:
        raise SystemExit("no BACKUP.json in %s: run 'run' first" % root)
    duck = Duck.find(args.duckdb, temp_dir=os.path.join(root, ".duckdb-tmp"), memory_limit=args.duckdb_memory)
    if not duck:
        raise SystemExit("merge-year needs the DuckDB CLI")
    y = str(args.year)
    if now_us() < year_end_us(y):
        raise SystemExit("%s has not ended" % y)
    schema = read_json(os.path.join(root, "schema", "tables.json"))
    rc = 0
    with Lock(root):
        for t in bk["config"]["tables"]:
            if t["mode"] != "chunked" or (args.table and t["table"] not in args.table):
                continue
            try:
                merge_one(root, t, schema["tables"][t["table"]], y, duck, args)
            except Failure as e:
                log("%s %s: REFUSED  %s" % (t["table"], y, e))
                rc = 1
        if not args.dry_run:
            write_sums(root)
    return rc


def merge_one(root, t, tschema, y, duck, args):
    mpath = os.path.join(root, t["dir"], t["prefix"] + ".manifest.json")
    man = read_json(mpath)
    if not man:
        raise Failure("no manifest")
    months = sorted(m for m in man["chunks"] if m[:4] == y)
    if not months:
        log("%s %s: no months" % (t["table"], y))
        return
    settle_us = int(t.get("settle_days", 1)) * DAY_US
    if now_us() < year_end_us(y) + settle_us:
        raise Failure("the year has not settled yet (settle_days %s)" % t.get("settle_days"))
    bad, files = [], []
    cols = None
    for m in months:
        e = man["chunks"][m]
        mrel = os.path.normpath(os.path.join(t["dir"], t["prefix"] + "_parquet", "%s_%s.parquet" % (t["prefix"], m)))
        if not e.get("settled") or e.get("stable") is False:
            bad.append("%s not settled/stable (run again)" % m)
        elif e.get("file") != mrel or not os.path.exists(os.path.join(root, mrel)):
            bad.append("%s has no monthly file (run with --refetch %s)" % (m, m))
        if cols is None:
            cols = e.get("columns")
        elif e.get("columns") != cols:
            bad.append("%s has different columns (schema changed mid-year: --refetch the older months)" % m)
        files.append(os.path.join(root, mrel))
    if bad:
        raise Failure("; ".join(bad))
    types = cols or tschema["types"]  # the columns the monthly files were written with
    unsafe = [c + " " + ty for c, ty in types if base_type(ty) not in DUCKDB_SAFE_TYPES]
    if unsafe:
        raise Failure("DuckDB would change these types: %s; keep the monthly files" % ", ".join(unsafe))
    ts = man["ts"]
    tmeta = tschema["table"]
    terms = fp_terms(types, ts, dict((c, ty) for c, ty in types).get(ts, "TIMESTAMP"))
    yrel = os.path.normpath(os.path.join(t["dir"], "%s_%s.parquet" % (t["prefix"], y)))
    out = os.path.join(root, yrel)
    if args.dry_run:
        log("%s %s: would merge %d months into %s" % (t["table"], y, len(months), yrel))
        return
    part = out + ".part"
    rm_f(part)
    log("%s %s: merging %d monthly files into %s" % (t["table"], y, len(files), yrel))
    t0 = time.time()
    try:
        duck.execute("COPY (SELECT * FROM read_parquet(%s)) TO %s (FORMAT parquet, COMPRESSION zstd)"
                     % (Duck.file_list(files), qlit(part)))
        a = duck_fp_by_day(duck, files, ts, terms)
        b = duck_fp_by_day(duck, [part], ts, terms)
        if a != b:
            diff = [d for d in sorted(set(a) | set(b)) if a.get(d) != b.get(d)]
            raise Failure("merged file differs from the monthly files on %s" % ", ".join(diff[:5]))
        for m in months:
            fpd = man["chunks"][m].get("fp_days")
            if fpd:
                for d, f in fpd.items():
                    v, diff = fp_verdict(f, b.get(d))
                    if v == "diff":
                        raise Failure("merged file differs from the source fingerprint on %s: %s" % (d, ", ".join(diff)))
        os.replace(part, out)
    except BaseException:
        rm_f(part)
        raise
    ent = {"merged_at": iso_now(), "months": months, "stale_months": [], "file": yrel,
           "bytes": os.path.getsize(out), "sha256": sha256_file(out), "rows": sum(f["rows"] for f in b.values())}
    man["years"][y] = ent
    for m in months:
        man["chunks"][m]["year"] = y
    log("  ok: %d rows, %s in %.0fs; verified per day against the monthly files" % (ent["rows"], human_bytes(ent["bytes"]), time.time() - t0))
    doomed = []
    if args.delete_months:
        for m in months:
            e = man["chunks"][m]
            doomed.append(e["file"])
            e["file"], e["bytes"], e["sha256"] = yrel, None, None
    # The manifest points at the year file before any monthly file goes: a crash in between leaves extra files,
    # never a manifest that names deleted ones.
    man["updated_at"] = iso_now()
    write_json(mpath, man)
    for f in doomed:
        if f != yrel:
            rm_f(os.path.join(root, f))
    if doomed:
        log("  monthly files deleted (--delete-months)")


# ---------------------------------------------------------------------------------------------------------------
# verify (local, no QuestDB)


def cmd_verify(args):
    root = os.path.abspath(args.root)
    bk = read_json(os.path.join(root, "BACKUP.json"))
    if not bk:
        raise SystemExit("no BACKUP.json in %s" % root)
    duck = Duck.find(args.duckdb, temp_dir=os.path.join(root, ".duckdb-tmp")) if args.deep else None
    if args.deep and not duck:
        raise SystemExit("--deep needs the DuckDB CLI")
    schema = read_json(os.path.join(root, "schema", "tables.json"))
    without = [w.rstrip("/") for w in args.without or []]
    problems, checked = [], 0
    for t in bk["tables"]:
        if not t.get("manifest") or in_dirs(t["dir"], without):
            continue
        man = read_json(os.path.join(root, t["manifest"]))
        if man is None:
            problems.append("%s: manifest missing" % t["manifest"])
            continue
        entries = []
        if man.get("full"):
            entries.append(("full", man["full"]))
        entries += sorted(man.get("chunks", {}).items())
        hashed = set()
        for key, e in entries:
            f = e.get("file")
            if not f:
                continue
            p = os.path.join(root, f)
            if not os.path.exists(p):
                problems.append("%s %s: %s missing" % (t["table"], key, f))
                continue
            sha = e.get("sha256") or (man["years"].get(e.get("year"), {}).get("sha256") if e.get("year") else None)
            if f not in hashed and sha:
                hashed.add(f)
                if sha256_file(p) != sha:
                    problems.append("%s %s: %s sha256 differs from the manifest" % (t["table"], key, f))
                    continue
                checked += 1
            if duck and key != "full" and e.get("fp_days"):
                types = schema["tables"][t["table"]]["types"]
                terms = chunk_terms(e, types, man["ts"])
                where = "WHERE epoch_us(%s) >= %d AND epoch_us(%s) < %d" % (
                    qident(man["ts"]), month_start_us(key), qident(man["ts"]), month_end_us(key))
                ffp = duck_fp_by_day(duck, [p], man["ts"], terms, where)
                for d in sorted(set(ffp) | set(e["fp_days"])):
                    v, diff = fp_verdict(e["fp_days"].get(d), ffp.get(d))
                    if v == "diff":
                        problems.append("%s %s: file fingerprint differs on %s (%s)" % (t["table"], key, d, ", ".join(diff[:4])))
                        break
        log("  %-22s %d file(s) checked" % (t["table"], len(hashed)))
    if problems:
        log("\nPROBLEMS:")
        for p in problems:
            log("  " + p)
        return 1
    log("\nOK: %d files match their sha256%s" % (checked, " and fingerprints" if duck else ""))
    return 0


def in_dirs(d, dirs):
    d = os.path.normpath(d)
    return any(d == w or d.startswith(w + "/") for w in (os.path.normpath(x) for x in dirs))


# ---------------------------------------------------------------------------------------------------------------
# restore


def cmd_restore(args):
    root = os.path.abspath(args.backup)
    bk = read_json(os.path.join(root, "BACKUP.json"))
    if not bk:
        raise SystemExit("no BACKUP.json in %s" % root)
    schema = read_json(os.path.join(root, "schema", "tables.json"))
    qdb = Qdb(args.url, timeout=3600)
    without = [w.rstrip("/") for w in args.without or []]
    only = set(args.tables.split(",")) if args.tables else None
    failures = []

    build = qdb.rows("SELECT build()")[0][0]
    log("restore  %s -> %s" % (root, qdb.url))
    log("  target: %s" % build)
    log("  backup: %s (taken %s)" % (bk["source_build"], bk["updated_at"]))
    if build.split(",")[0] != bk["source_build"].split(",")[0]:
        log("  WARNING: QuestDB versions differ; the schema check below decides")
    existing = {r["table_name"]: r for r in qdb.dicts("SELECT table_name, table_type, dedup FROM tables()")}
    cfg_tables = [t for t in bk["config"]["tables"] if t["mode"] != "skip" and (not only or t["table"] in only)]
    clash = [t["table"] for t in cfg_tables if t["table"] in existing]
    if clash and not args.resume:
        raise SystemExit("the target already has %s; restore into an empty QuestDB, or pass --resume to continue "
                         "an earlier restore" % ", ".join(clash))
    # The copy root must be this backup: read one small file and compare its row count with the manifest.
    probe = find_probe_file(root, bk, without)
    if probe:
        try:
            n = to_int(qdb.rows("SELECT count() FROM read_parquet(%s)" % qlit(probe[0]))[0][0])
        except QdbError as e:
            raise SystemExit("the target cannot read the backup files (%s).\nStart QuestDB with this backup mounted "
                             "read-only as its sql copy root, e.g. -v %s:/backup:ro -e QDB_CAIRO_SQL_COPY_ROOT=/backup"
                             % (e, root))
        if probe[1] is not None and n != probe[1]:
            raise SystemExit("the target's copy root is not this backup (%s has %d rows, the manifest says %d)"
                             % (probe[0], n, probe[1]))
    if args.dry_run:
        log("  (dry run) tables: %s" % ", ".join(t["table"] for t in cfg_tables))
        return 0

    # 1. tables, without DEDUP (rows with equal keys in the backup are all kept; DEDUP is enabled after the load)
    order = sorted(cfg_tables, key=lambda t: {"full": 0, "schema": 1, "chunked": 2}[t["mode"]])
    for t in order:
        name = t["table"]
        ts = schema["tables"].get(name)
        if ts is None:
            failures.append("%s: no DDL in the backup" % name)
            continue
        if name in existing:
            if existing[name].get("dedup") and ts["dedup"]:
                qdb.exec("ALTER TABLE %s DEDUP DISABLE" % qident(name))
            continue
        qdb.exec(ts["create"])
        log("  created %s" % name)

    # 2. data
    totals = {}
    for t in order:
        name = t["table"]
        if t["mode"] == "schema" or name not in schema["tables"]:
            continue
        if in_dirs(t["dir"], without):
            log("  %s: no data (--without %s)" % (name, t["dir"]))
            continue
        man = read_json(os.path.join(root, t["dir"], t["prefix"] + ".manifest.json"))
        if man is None:
            failures.append("%s: manifest missing" % name)
            continue
        try:
            if t["mode"] == "full":
                totals[name] = restore_full(qdb, root, name, man, schema, args)
            else:
                totals[name] = restore_chunked(qdb, root, name, man, schema, args, failures)
        except (Failure, QdbError) as e:
            failures.append("%s: %s" % (name, e))
            log("  %s FAILED: %s" % (name, e))

    # 3. DEDUP on, WAL applied
    for t in order:
        ts = schema["tables"].get(t["table"])
        if ts and ts["dedup"]:
            qdb.exec(ts["dedup"])
    for t in order:
        if not wait_wal_long(qdb, t["table"]):
            failures.append("%s: WAL not applied" % t["table"])

    # 4. views
    if not only:
        have = {r[0] for r in qdb.rows("SELECT view_name FROM views()")}
        pending = [v for v in schema["views"] if v["name"] not in have]
        for _ in range(len(pending) + 1):
            left = []
            for v in pending:
                try:
                    qdb.exec(v["sql"])
                    log("  view %s" % v["name"])
                except QdbError:
                    left.append(v)
            if not left or len(left) == len(pending):
                pending = left
                break
            pending = left
        for v in pending:
            failures.append("view %s could not be created" % v["name"])

    # 5. verify
    log("\nverify")
    tabs = {r["table_name"]: r for r in qdb.dicts("SELECT * FROM tables()")}
    for t in order:
        name = t["table"]
        want = schema["tables"].get(name)
        if not want or name not in tabs:
            failures.append("%s: missing on the target" % name)
            continue
        got_t = table_whitelist(tabs[name])
        got_c = columns_whitelist(qdb.dicts("SELECT * FROM table_columns(%s)" % qlit(name)))
        if got_t != want["table"]:
            failures.append("%s: table settings differ: %s" % (name, dict_diff(want["table"], got_t)))
        if got_c != want["columns"]:
            failures.append("%s: columns differ: %s" % (name, list_diff(want["columns"], got_c)))
        if name in totals:
            n = to_int(qdb.rows("SELECT count() FROM %s" % qident(name))[0][0])
            if n != totals[name]:
                failures.append("%s: %d rows on the target, %d expected" % (name, n, totals[name]))
            else:
                log("  %-22s %d rows, schema ok" % (name, n))
        else:
            log("  %-22s schema ok" % name)
    if not only:
        for v in schema["views"]:
            try:
                qdb.rows("SELECT * FROM %s LIMIT 1" % qident(v["name"]))
            except QdbError as e:
                failures.append("view %s: %s" % (v["name"], e))
    if failures:
        log("\nRESTORE NOT COMPLETE (%d problem(s)):" % len(failures))
        for f in failures:
            log("  " + f)
        log("Fix the cause and rerun with --resume: days that already match are skipped.")
        return 1
    log("\nRESTORE OK: every table matches the backup (schema, rows and per-day fingerprints).")
    return 0


def dict_diff(a, b):
    return ", ".join("%s %r != %r" % (k, a.get(k), b.get(k)) for k in sorted(set(a) | set(b)) if a.get(k) != b.get(k))


def list_diff(a, b):
    out = []
    for i in range(max(len(a), len(b))):
        x = a[i] if i < len(a) else None
        y = b[i] if i < len(b) else None
        if x != y:
            out.append("%r != %r" % (x, y))
    return "; ".join(out[:3])


def wait_wal_long(qdb, table, limit_s=3600):
    try:
        return qdb.wait_wal(table, limit_s)
    except Failure as e:
        log("  " + str(e))
        return False


def find_probe_file(root, bk, without):
    for t in bk["tables"]:
        if t.get("mode") != "full" or in_dirs(t["dir"], without):
            continue
        man = read_json(os.path.join(root, t["manifest"]))
        if man and man.get("full", {}).get("file") and os.path.exists(os.path.join(root, man["full"]["file"])):
            return man["full"]["file"], man["full"].get("rows")
    for t in bk["tables"]:
        if t.get("mode") != "chunked" or in_dirs(t["dir"], without):
            continue
        man = read_json(os.path.join(root, t["manifest"]))
        for m, e in sorted((man or {}).get("chunks", {}).items()):
            if e.get("file") and not e.get("year") and os.path.exists(os.path.join(root, e["file"])):
                return e["file"], (e.get("meta") or {}).get("rows") if e.get("settled") and e.get("stable") else None
    return None


def insert_select(qdb, name, want_types, rel, where=""):
    """INSERT INTO t (cols) SELECT cast(...) FROM read_parquet(rel): by column name, with casts where the file type
    differs (DuckDB-merged files: VARCHAR for STRING, ...). Columns the file lacks stay NULL (schema drift)."""
    file_cols = dict(qdb.columns_of("SELECT * FROM read_parquet(%s) LIMIT 0" % qlit(rel)))
    names, exprs = [], []
    for c, ty in want_types:
        if c not in file_cols:
            continue
        names.append(qident(c))
        b = base_type(ty)
        if file_cols[c] == ty or b == "SYMBOL":
            exprs.append(qident(c))
        else:
            exprs.append("cast(%s AS %s)" % (qident(c), ty))
    extra = sorted(set(file_cols) - {c for c, _ in want_types})
    if extra:
        raise Failure("%s has columns the table does not: %s" % (rel, ", ".join(extra)))
    qdb.exec("INSERT INTO %s (%s) SELECT %s FROM read_parquet(%s) %s" % (
        qident(name), ", ".join(names), ", ".join(exprs), qlit(rel), where), timeout=3600)


def check_sha(root, rel, sha, args, done):
    if args.no_sha or not sha or rel in done:
        return
    if sha256_file(os.path.join(root, rel)) != sha:
        raise Failure("%s does not match its sha256 in the manifest (damaged copy?)" % rel)
    done.add(rel)


def restore_full(qdb, root, name, man, schema, args):
    e = man.get("full")
    if not e:
        raise Failure("nothing in the manifest")
    types = schema["tables"][name]["types"]
    ts = man.get("ts")
    terms = fp_terms(types, ts)
    file_fp = qdb_fp_whole(qdb, "read_parquet(%s)" % qlit(e["file"]), terms)
    cur = qdb_fp_whole(qdb, qident(name), terms)
    if cur == file_fp and cur["rows"] > 0:
        log("  %-22s already restored (%d rows)" % (name, cur["rows"]))
        return cur["rows"]
    check_sha(root, e["file"], e.get("sha256"), args, set())
    if cur["rows"]:
        qdb.exec("TRUNCATE TABLE %s" % qident(name))
        wait_wal_long(qdb, name)
    insert_select(qdb, name, types, e["file"])
    wait_wal_long(qdb, name)
    got = qdb_fp_whole(qdb, qident(name), terms)
    if fp_diff(got, file_fp):
        raise Failure("after the load the table fingerprint differs from the file: %s" % ", ".join(fp_diff(got, file_fp)))
    if e.get("fp") and fp_diff(got, e["fp"]):
        raise Failure("the file differs from the source fingerprint taken at backup time")
    log("  %-22s %d rows" % (name, got["rows"]))
    return got["rows"]


def restore_chunked(qdb, root, name, man, schema, args, failures):
    types = schema["tables"][name]["types"]
    ts = man["ts"]
    terms = fp_terms(types, ts, dict(types).get(ts, "TIMESTAMP"))
    day_parts = man.get("partition_by") in ("DAY", "HOUR")
    total = 0
    hashed = set()
    t0 = time.time()
    for m, e in sorted(man["chunks"].items()):
        if (args.from_month and m < args.from_month) or (args.to_month and m > args.to_month):
            continue
        rel = e.get("file")
        if not rel or not os.path.exists(os.path.join(root, rel)):
            failures.append("%s %s: file missing" % (name, m))
            continue
        sha = e.get("sha256") or (man["years"].get(e.get("year"), {}).get("sha256") if e.get("year") else None)
        try:
            check_sha(root, rel, sha, args, hashed)
            src = "read_parquet(%s)" % qlit(rel)
            terms = chunk_terms(e, types, ts)
            file_fp = qdb_fp_by_day(qdb, src, ts, terms, m)
            cur = qdb_fp_by_day(qdb, qident(name), ts, terms, m)
            todo = [d for d in sorted(file_fp) if cur.get(d) != file_fp[d]]
            stray = [d for d in cur if d not in file_fp]
            if stray:
                raise Failure("the target has rows on days the backup does not: %s" % ", ".join(stray[:3]))
            dirty = [d for d in todo if cur.get(d)]
            if dirty:
                if not day_parts:
                    raise Failure("partly loaded and the table is not partitioned by DAY/HOUR: drop the table and "
                                  "restore it again (--tables %s)" % name)
                for d in dirty:
                    qdb.exec("ALTER TABLE %s DROP PARTITION WHERE %s IN %s" % (qident(name), qident(ts), qlit(d)))
                wait_wal_long(qdb, name)
            for d in todo:
                insert_select(qdb, name, types, rel, "WHERE %s IN %s" % (qident(ts), qlit(d)))
            if todo:
                if not wait_wal_long(qdb, name):
                    raise Failure("WAL not applied")
                cur = qdb_fp_by_day(qdb, qident(name), ts, terms, m)
            bad = [d for d in sorted(file_fp) if fp_diff(cur.get(d), file_fp[d])]
            if bad:
                raise Failure("table differs from the file after the load on %s" % ", ".join(bad[:5]))
            src_bad = []
            if e.get("fp_days") and e.get("stable"):
                src_bad = [d for d, f in e["fp_days"].items() if fp_diff(cur.get(d), f)]
            if src_bad:
                raise Failure("restored data differs from the source fingerprint taken at backup time on %s"
                              % ", ".join(src_bad[:5]))
            rows = sum(f["rows"] for f in file_fp.values())
            total += rows
            log("  %-22s %s  %10d rows  %s  (%.0fs)" % (name, m, rows,
                "loaded %d day(s)" % len(todo) if todo else "already there", time.time() - t0))
        except (Failure, QdbError) as ex:
            failures.append("%s %s: %s" % (name, m, ex))
            log("  %-22s %s  FAILED: %s" % (name, m, ex))
    return total


# ---------------------------------------------------------------------------------------------------------------
# schema


def cmd_schema(args):
    cfg = load_config(args.config) if args.config else {"tables": []}
    qdb = Qdb(args.url)
    tables = cfg["tables"]
    if not tables:
        tables = [{"table": r["table_name"], "mode": "schema"} for r in qdb.dicts("SELECT table_name, table_type FROM tables()")
                  if r["table_type"] != "V"]
    capture_schema(qdb, os.path.abspath(args.root), tables)
    log("schema written to %s/schema" % os.path.abspath(args.root))
    return 0


# ---------------------------------------------------------------------------------------------------------------


def main(argv=None):
    ap = argparse.ArgumentParser(prog="qdb-backup.py", description=__doc__.split("\n\n")[0].strip(),
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd")

    r = sub.add_parser("run", help="export what changed, capture the schema, write manifests")
    r.add_argument("--config", required=True)
    r.add_argument("--root", required=True, help="backup directory")
    r.add_argument("--url", default=os.environ.get("QDB", "http://localhost:9000"))
    r.add_argument("--only", action="append", help="only this table (repeatable)")
    r.add_argument("--refetch", action="append", metavar="YYYY-MM", help="re-export this month (repeatable)")
    r.add_argument("--dry-run", action="store_true", help="show what would be exported; downloads nothing")
    r.add_argument("--deep-check", action="store_true", help="also fingerprint settled months against the source")
    r.add_argument("--deep-limit", type=int, default=3, help="months per table for --deep-check (default 3)")
    r.add_argument("--timeout", type=int, help="per-request download timeout in seconds (default: the config's, 280)")
    r.add_argument("--accept-shrink", action="store_true", help="replace a settled month even if it lost >10%% rows")
    r.add_argument("--max-new", type=int, default=3, metavar="N",
                   help="stop when more than N settled months per table have no file yet (default 3; raise it for "
                        "a first backup)")
    r.add_argument("--full-max-rows", type=int, default=10000000)
    r.add_argument("--min-free-gb", type=float, default=10.0)
    r.add_argument("--max-clock-skew", type=int, default=300)
    r.add_argument("--note", action="append", metavar="KEY=VALUE", help="provenance recorded in BACKUP.json")
    r.add_argument("--duckdb", help="path of the DuckDB CLI (default: duckdb on PATH)")
    r.add_argument("--duckdb-memory", default=None, help="DuckDB memory_limit, e.g. 4GB")
    r.add_argument("--no-duckdb", action="store_true", help="run without DuckDB: files are not checked locally and "
                   "existing files are adopted by their file time (checked on the next run with DuckDB)")
    r.add_argument("-v", "--verbose", action="store_true")

    m = sub.add_parser("merge-year", help="merge one year's monthly files into <prefix>_<YYYY>.parquet")
    m.add_argument("--root", required=True)
    m.add_argument("--year", required=True)
    m.add_argument("--table", action="append")
    m.add_argument("--delete-months", action="store_true", help="delete the monthly files after the check passed")
    m.add_argument("--dry-run", action="store_true")
    m.add_argument("--duckdb")
    m.add_argument("--duckdb-memory", default="4GB")

    v = sub.add_parser("verify", help="check files against the manifests (no QuestDB needed)")
    v.add_argument("--root", required=True)
    v.add_argument("--deep", action="store_true", help="also compare DuckDB fingerprints with the manifests")
    v.add_argument("--without", action="append", metavar="DIR", help="skip objects stored under DIR")
    v.add_argument("--duckdb")

    s = sub.add_parser("restore", help="rebuild a QuestDB from a backup")
    s.add_argument("--backup", required=True)
    s.add_argument("--url", default="http://localhost:9000")
    s.add_argument("--tables", help="comma-separated subset (views are then not created)")
    s.add_argument("--without", action="append", metavar="DIR", help="create the tables stored under DIR but load no data")
    s.add_argument("--from", dest="from_month", metavar="YYYY-MM")
    s.add_argument("--to", dest="to_month", metavar="YYYY-MM")
    s.add_argument("--resume", action="store_true", help="continue an earlier restore into the same target")
    s.add_argument("--no-sha", action="store_true", help="skip the sha256 check of each file before loading")
    s.add_argument("--dry-run", action="store_true")

    c = sub.add_parser("schema", help="capture the schema only")
    c.add_argument("--root", required=True)
    c.add_argument("--url", default=os.environ.get("QDB", "http://localhost:9000"))
    c.add_argument("--config")

    args = ap.parse_args(argv)
    if not args.cmd:
        ap.print_help()
        return 2
    try:
        return {"run": cmd_run, "merge-year": cmd_merge_year, "verify": cmd_verify, "restore": cmd_restore,
                "schema": cmd_schema}[args.cmd](args)
    except QdbError as e:
        log("ERROR: %s" % e)
        return 1
    except KeyboardInterrupt:
        log("interrupted")
        return 130


if __name__ == "__main__":
    sys.exit(main())
