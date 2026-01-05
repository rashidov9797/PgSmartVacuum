
#!/usr/bin/env python3
"""
Detect PostgreSQL table bloat using pgstattuple and remediate ONLY with VACUUM ANALYZE.

Notes:
- Comments and variable names are in English.
- Output messages are in English.
- Defaults are defined in-code; optional env vars can override them.
- QUIET mode: by default, nothing is printed to the terminal; everything goes to a per-run log file.
"""

import os
import sys
import time
from datetime import datetime
from typing import Optional, List

import psycopg2
from psycopg2 import sql
from psycopg2 import OperationalError, Error


# ----------------------------
# Configuration (DEFAULTS - edit here)
# ----------------------------

DEFAULT_PGHOST = "127.0.0.1"
DEFAULT_PGPORT = 5432
DEFAULT_PGDATABASE = "postgres"
DEFAULT_PGUSER = "postgres"
DEFAULT_PGPASSWORD = ""  # Prefer ~/.pgpass instead of hardcoding a password

# Multi-schema: comma-separated list ("" => all user schemas)
# Example: "public,test"
DEFAULT_TARGET_SCHEMAS = ""

# Threshold (pgstattuple.dead_tuple_percent). Default = 2%.
DEFAULT_DEAD_TUPLE_PERCENT_THRESHOLD = 2.0

# Prefilter (approx) to reduce pgstattuple scans
DEFAULT_APPROX_PREFILTER_DEAD_PCT = 1.0
DEFAULT_MAX_TABLES_TO_CHECK = 200
DEFAULT_TOP_STATS_LIMIT = 10

# Session safety timeouts (milliseconds)
DEFAULT_LOCK_TIMEOUT_MS = 2000
DEFAULT_STATEMENT_TIMEOUT_MS = 600000

DEFAULT_APPLICATION_NAME = "bloat_vacuum_analyze"

# Log directory
DEFAULT_LOG_DIR = "/home/postgres/bloating_log"

# Logging style
DEFAULT_LOG_INCLUDE_TIMESTAMP = False
DEFAULT_LOG_WIDTH = 110

# Quiet mode: if True, do not print to terminal, only write to log file
DEFAULT_QUIET = True


# ----------------------------
# Configuration (ENV overrides)
# ----------------------------

PGHOST = os.getenv("PGHOST", DEFAULT_PGHOST)
PGPORT = int(os.getenv("PGPORT", str(DEFAULT_PGPORT)))
PGDATABASE = os.getenv("PGDATABASE", DEFAULT_PGDATABASE)
PGUSER = os.getenv("PGUSER", DEFAULT_PGUSER)
PGPASSWORD = os.getenv("PGPASSWORD", DEFAULT_PGPASSWORD)

TARGET_SCHEMAS = os.getenv("TARGET_SCHEMAS", DEFAULT_TARGET_SCHEMAS)

DEAD_TUPLE_PERCENT_THRESHOLD = float(
    os.getenv("DEAD_TUPLE_PERCENT_THRESHOLD", str(DEFAULT_DEAD_TUPLE_PERCENT_THRESHOLD))
)
APPROX_PREFILTER_DEAD_PCT = float(os.getenv("APPROX_PREFILTER_DEAD_PCT", str(DEFAULT_APPROX_PREFILTER_DEAD_PCT)))
MAX_TABLES_TO_CHECK = int(os.getenv("MAX_TABLES_TO_CHECK", str(DEFAULT_MAX_TABLES_TO_CHECK)))
TOP_STATS_LIMIT = int(os.getenv("TOP_STATS_LIMIT", str(DEFAULT_TOP_STATS_LIMIT)))

LOCK_TIMEOUT_MS = int(os.getenv("LOCK_TIMEOUT_MS", str(DEFAULT_LOCK_TIMEOUT_MS)))
STATEMENT_TIMEOUT_MS = int(os.getenv("STATEMENT_TIMEOUT_MS", str(DEFAULT_STATEMENT_TIMEOUT_MS)))

APPLICATION_NAME = os.getenv("APPLICATION_NAME", DEFAULT_APPLICATION_NAME)

LOG_DIR = os.getenv("LOG_DIR", DEFAULT_LOG_DIR).rstrip("/")
LOG_INCLUDE_TIMESTAMP = os.getenv("LOG_INCLUDE_TIMESTAMP", str(int(DEFAULT_LOG_INCLUDE_TIMESTAMP))).strip().lower() in (
    "1",
    "true",
    "yes",
    "y",
)
LOG_WIDTH = int(os.getenv("LOG_WIDTH", str(DEFAULT_LOG_WIDTH)))

QUIET = os.getenv("QUIET", str(int(DEFAULT_QUIET))).strip().lower() in ("1", "true", "yes", "y")

RUN_TS = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = os.path.join(LOG_DIR, f"bloat_vacuum_{RUN_TS}.log")


def parse_target_schemas(raw: str) -> List[str]:
    """Parse comma-separated schema list. Empty -> [] meaning 'all user schemas'."""
    if not raw:
        return []
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]


TARGET_SCHEMA_LIST = parse_target_schemas(TARGET_SCHEMAS)


def _stamp() -> str:
    """Return timestamp prefix or empty, depending on LOG_INCLUDE_TIMESTAMP."""
    if not LOG_INCLUDE_TIMESTAMP:
        return ""
    return time.strftime("%Y-%m-%d %H:%M:%S ")  # trailing space


def _write_log_line(out: str) -> None:
    """Write a line to the per-run log file."""
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(out + "\n")
    except Exception:
        # Never fail the script due to logging issues
        pass


def log(line: str = "") -> None:
    """Append to the log file; optionally print to terminal depending on QUIET."""
    out = (f"{_stamp()}{line}" if line else f"{_stamp()}").rstrip()

    # Always write to file
    _write_log_line(out)

    # Print only if not quiet
    if not QUIET:
        print(out)


def rule(char: str = "#", title: str = "", width: Optional[int] = None) -> None:
    """Print a visual separator line, optionally with a centered title."""
    w = width if width is not None else LOG_WIDTH
    c = (char or "#")[0]
    if not title:
        log(c * w)
        return

    pad_total = max(0, w - (len(title) + 4))
    left = pad_total // 2
    right = pad_total - left
    log(f"{c * left}  {title}  {c * right}")


def connect():
    """Create a PostgreSQL connection (autocommit enabled for VACUUM)."""
    conn = psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        connect_timeout=10,
        application_name=APPLICATION_NAME,
    )
    conn.autocommit = True
    return conn


def set_session_timeouts(conn):
    """Set session-level timeouts to avoid hanging on locks or long statements."""
    with conn.cursor() as cur:
        cur.execute("SET lock_timeout = %s;", (f"{LOCK_TIMEOUT_MS}ms",))
        cur.execute("SET statement_timeout = %s;", (f"{STATEMENT_TIMEOUT_MS}ms",))
        cur.execute("SET idle_in_transaction_session_timeout = %s;", ("60s",))


def ensure_pgstattuple(conn) -> bool:
    """Ensure pgstattuple extension exists; try to create it if missing."""
    with conn.cursor() as cur:
        cur.execute("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pgstattuple');")
        exists = cur.fetchone()[0]

    if exists:
        log("OK: pgstattuple extension is present.")
        return True

    log("WARN: pgstattuple extension not found. Trying to create it...")
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pgstattuple;")
        log("OK: pgstattuple extension created (or already existed).")
        return True
    except Error as e:
        log(f"ERROR: could not create pgstattuple extension: {e.pgerror or str(e)}")
        return False


def log_top_table_stats(conn) -> None:
    """Log top tables by n_dead_tup with vacuum/analyze stats."""
    rule("#", "TOP TABLES (dead tuples + vacuum/analyze stats)")

    query = """
    SELECT
        n.nspname AS schema_name,
        c.relname AS table_name,
        COALESCE(s.n_live_tup, 0) AS n_live_tup,
        COALESCE(s.n_dead_tup, 0) AS n_dead_tup,
        CASE
            WHEN (COALESCE(s.n_live_tup, 0) + COALESCE(s.n_dead_tup, 0)) = 0 THEN 0.0
            ELSE (COALESCE(s.n_dead_tup, 0) * 100.0) /
                 (COALESCE(s.n_live_tup, 0) + COALESCE(s.n_dead_tup, 0))
        END AS approx_dead_pct,
        s.last_vacuum,
        s.last_autovacuum,
        s.last_analyze,
        s.last_autoanalyze
    FROM pg_stat_user_tables s
    JOIN pg_class c ON c.oid = s.relid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname NOT LIKE 'pg_toast%%'
    """

    params: List[object] = []
    if TARGET_SCHEMA_LIST:
        query += " AND n.nspname = ANY(%s)"
        params.append(TARGET_SCHEMA_LIST)

    query += """
    ORDER BY COALESCE(s.n_dead_tup, 0) DESC
    LIMIT %s;
    """
    params.append(TOP_STATS_LIMIT)

    with conn.cursor() as cur:
        cur.execute(query, tuple(params))
        rows = cur.fetchall()

    if not rows:
        log("(no user tables returned by pg_stat_user_tables)")
        rule("#")
        log("")
        return

    log(
        f"{'schema':<20} {'table':<28} {'live':>10} {'dead':>10} {'dead%':>7}  "
        f"{'last_vacuum':<19} {'last_autovac':<19} {'last_analyze':<19} {'last_autoanl':<19}"
    )
    log("-" * LOG_WIDTH)

    for r in rows:
        schema_name, table_name, live, dead, dead_pct, lv, lav, la, laa = r
        log(
            f"{schema_name:<20} {table_name:<28} {int(live):>10} {int(dead):>10} {dead_pct:>6.2f}  "
            f"{str(lv)[:19]:<19} {str(lav)[:19]:<19} {str(la)[:19]:<19} {str(laa)[:19]:<19}"
        )

    rule("#")
    log("")


def get_candidate_tables(conn):
    """
    Prefilter candidates using pg_stat_all_tables approximate dead tuple percent.

    IMPORTANT (psycopg2): any '%' in the query must be escaped as '%%' unless it is a %s placeholder.
    """
    query = """
    WITH candidates AS (
        SELECT
            n.nspname AS schema_name,
            c.relname AS table_name,
            format('%%I.%%I', n.nspname, c.relname) AS fqname,
            COALESCE(s.n_live_tup, 0) AS n_live_tup,
            COALESCE(s.n_dead_tup, 0) AS n_dead_tup,
            CASE
                WHEN (COALESCE(s.n_live_tup, 0) + COALESCE(s.n_dead_tup, 0)) = 0 THEN 0.0
                ELSE (COALESCE(s.n_dead_tup, 0) * 100.0) /
                     (COALESCE(s.n_live_tup, 0) + COALESCE(s.n_dead_tup, 0))
            END AS approx_dead_pct
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_stat_all_tables s ON s.relid = c.oid
        WHERE c.relkind = 'r'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND n.nspname NOT LIKE 'pg_toast%%'
    )
    SELECT schema_name, table_name, fqname, approx_dead_pct
    FROM candidates
    WHERE approx_dead_pct >= %s
    """

    params: List[object] = [APPROX_PREFILTER_DEAD_PCT]

    if TARGET_SCHEMA_LIST:
        query += " AND schema_name = ANY(%s)"
        params.append(TARGET_SCHEMA_LIST)

    query += """
    ORDER BY approx_dead_pct DESC
    LIMIT %s;
    """
    params.append(MAX_TABLES_TO_CHECK)

    with conn.cursor() as cur:
        cur.execute(query, tuple(params))
        return cur.fetchall()


def analyze_table(conn, schema_name, table_name):
    """Run ANALYZE on a table to refresh planner statistics."""
    stmt = sql.SQL("ANALYZE {}.{};").format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
    )
    with conn.cursor() as cur:
        cur.execute(stmt)


def get_pgstattuple_dead_percent(conn, fqname):
    """Return dead_tuple_percent using pgstattuple for a qualified name."""
    with conn.cursor() as cur:
        cur.execute("SELECT (pgstattuple(%s::regclass)).dead_tuple_percent;", (fqname,))
        return float(cur.fetchone()[0])


def vacuum_analyze_table(conn, schema_name, table_name):
    """Run VACUUM (ANALYZE) on a table."""
    stmt = sql.SQL("VACUUM (ANALYZE) {}.{};").format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
    )
    with conn.cursor() as cur:
        cur.execute(stmt)


def main():
    # Write everything to a per-run log file; do not print if QUIET=True.
    rule("*", "BLOAT VACUUM RUN START")
    log(f"DB: host={PGHOST} port={PGPORT} db={PGDATABASE} user={PGUSER}")
    log(f"Target schemas: {', '.join(TARGET_SCHEMA_LIST) if TARGET_SCHEMA_LIST else '(all user schemas)'}")
    log(f"Threshold: pgstattuple dead_tuple_percent >= {DEAD_TUPLE_PERCENT_THRESHOLD}%")
    log(f"Prefilter: approx_dead_pct >= {APPROX_PREFILTER_DEAD_PCT}% (max {MAX_TABLES_TO_CHECK} tables)")
    log(f"Timeouts: lock_timeout={LOCK_TIMEOUT_MS}ms, statement_timeout={STATEMENT_TIMEOUT_MS}ms")
    log(f"Log file: {LOG_FILE}")
    rule("*")
    log("")

    try:
        conn = connect()
    except OperationalError as e:
        rule("!", "FATAL")
        log(f"ERROR: could not connect to the database: {str(e)}")
        rule("!")
        sys.exit(2)

    try:
        set_session_timeouts(conn)

        rule("#", "EXTENSION CHECK")
        if not ensure_pgstattuple(conn):
            rule("!", "FATAL")
            log("ERROR: Cannot continue: pgstattuple extension is missing.")
            rule("!")
            sys.exit(3)
        rule("#")
        log("")

        log_top_table_stats(conn)

        rule("#", "CANDIDATE SELECTION")
        candidates = get_candidate_tables(conn)
        log(f"Candidate tables found: {len(candidates)}")
        rule("#")
        log("")

        if not candidates:
            rule("*", "RUN COMPLETE")
            log("Nothing to do: no candidates matched the prefilter.")
            rule("*")
            return

        checked = 0
        analyzed = 0
        remediated = 0
        skipped = 0

        rule("#", "PROCESSING CANDIDATE TABLES")
        for schema_name, table_name, fqname, approx_dead_pct in candidates:
            checked += 1
            rule("-", f"[{checked}/{len(candidates)}] {fqname} (approx_dead%={approx_dead_pct:.2f})")

            try:
                try:
                    log("Step 1: ANALYZE (refresh stats)")
                    analyze_table(conn, schema_name, table_name)
                    analyzed += 1
                    log("ANALYZE: OK")
                except Error as e:
                    skipped += 1
                    log(f"ANALYZE: SKIP (error/lock) -> {e.pgerror or str(e)}")
                    log("")
                    continue

                log("Step 2: pgstattuple measurement")
                dead_pct = get_pgstattuple_dead_percent(conn, fqname)
                log(f"pgstattuple dead_tuple_percent = {dead_pct:.2f}%")

                if dead_pct < DEAD_TUPLE_PERCENT_THRESHOLD:
                    log("Result: below threshold -> VACUUM skipped")
                    log("")
                    continue

                log("Step 3: VACUUM (ANALYZE)")
                start_ts = time.time()
                try:
                    vacuum_analyze_table(conn, schema_name, table_name)
                    elapsed = time.time() - start_ts
                    remediated += 1
                    log(f"VACUUM (ANALYZE): OK (time={elapsed:.1f}s)")
                    log("")
                except Error as e:
                    skipped += 1
                    log(f"VACUUM (ANALYZE): SKIP (error/lock) -> {e.pgerror or str(e)}")
                    log("")

            except Error as e:
                skipped += 1
                log(f"pgstattuple: SKIP (error/lock) -> {e.pgerror or str(e)}")
                log("")
            except Exception as e:
                skipped += 1
                log(f"Unexpected error: SKIP -> {str(e)}")
                log("")

        rule("#")
        log("")

        rule("*", "SUMMARY")
        log(f"Checked: {checked}")
        log(f"ANALYZE executed: {analyzed}")
        log(f"VACUUM (ANALYZE) executed: {remediated}")
        log(f"Skipped (errors/locks): {skipped}")
        rule("*")

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()

