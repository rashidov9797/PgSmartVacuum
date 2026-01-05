

# 🐘 PG Smart VACUUM
 
A small, safety-first automation script that detects PostgreSQL table bloat using the `pgstattuple` extension and remediates it **only** via `VACUUM (ANALYZE)`.

This tool is designed for production:
- Uses `lock_timeout` and `statement_timeout` to avoid hanging sessions
- Skips locked/erroring tables gracefully (no crashes)
- Logs results into a per-run timestamped log file
- Optional multi-schema filtering
- Cron-friendly “quiet mode” (log-only)

> **Important:** This tool does **NOT** run `VACUUM FULL`. It only runs `VACUUM (ANALYZE)`.

---

## Features

- **Accurate bloat signal**: `pgstattuple(...).dead_tuple_percent`
- **Prefiltering**: uses `pg_stat_*` estimates to reduce expensive scans
- **Safety timeouts**: session `lock_timeout` + `statement_timeout`
- **Multi-schema support**: target specific schemas (e.g. `public,test`)
- **Quiet mode**: writes logs only (ideal for cron)
- **Per-run logs**: `bloat_vacuum_YYYYMMDD_HHMMSS.log`

---

## Requirements

- PostgreSQL **17+**
- `pgstattuple` extension available (script can attempt `CREATE EXTENSION`)
- Python **3.6+**
- `psycopg2` (or `psycopg2-binary`)

---

## Install (Rocky/RHEL-like)

```bash
# As postgres user
sudo -iu postgres

# Create a venv (recommended)
python3 -m venv /home/postgres/pg_bloat_env
source /home/postgres/pg_bloat_env/bin/activate

pip install --upgrade pip
pip install psycopg2-binary
````

---

## Database Setup

```sql
CREATE EXTENSION IF NOT EXISTS pgstattuple;
```

---

## Configuration

The script has in-code defaults, but you can override via environment variables if you prefer.

Common variables:

* `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`
* `TARGET_SCHEMAS` — comma-separated list, e.g. `public,test` (empty = all user schemas)
* `DEAD_TUPLE_PERCENT_THRESHOLD`
* `LOCK_TIMEOUT_MS`, `STATEMENT_TIMEOUT_MS`
* `LOG_DIR`
* `QUIET` — `1` (default) writes only to log file, no terminal output

---

## Run (Manual)

```bash
/home/postgres/pg_bloat_env/bin/python bloat_vacuum.py
```

To show output in terminal while testing:

```bash
QUIET=0 /home/postgres/pg_bloat_env/bin/python bloat_vacuum.py
```

---

## Run via Cron (Every Saturday 11:00)

```bash
sudo -iu postgres
crontab -e
```

Add:

```cron
0 11 * * 6 /home/postgres/pg_bloat_env/bin/python /path/to/bloat_vacuum.py >/dev/null 2>&1
```

Logs will be created per run in:

```
/home/postgres/bloating_log/bloat_vacuum_YYYYMMDD_HHMMSS.log
```

<img width="1128" height="820" alt="image" src="https://github.com/user-attachments/assets/afb8fc81-b3ca-4e11-954f-b767ea15a390" />



---

## How It Works

1. Ensures `pgstattuple` exists (checks, attempts creation if allowed)
2. Logs “Top N” tables by `n_dead_tup` including vacuum/analyze stats
3. Prefilters candidate tables using `pg_stat_*` estimates
4. For each candidate:

   * Runs `ANALYZE` first (refresh stats)
   * Measures `pgstattuple.dead_tuple_percent`
   * If above threshold -> runs `VACUUM (ANALYZE)` only

```
```

