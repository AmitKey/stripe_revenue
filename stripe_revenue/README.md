# stripe_revenue

dbt project that models DoorLoop's addon revenue from Stripe. Takes three raw source tables (charges, activity events, journal entries) and produces mart tables Finance and Analytics can query directly.

---

## What's in here

```
seeds/          raw source data (CSV) loaded into the database
models/
  staging/      rename + light cleaning of raw tables (views)
  intermediate/ joins and classifications (views)
  marts/        final tables for reporting (tables)
```

### Mart tables

| Table | One row = | Purpose |
|---|---|---|
| `mart_addon_revenue_events` | Stripe balance transaction | Every financial event with revenue category, date dims, and tenant context |
| `mart_addon_revenue_by_charge` | Stripe charge | Charge-level P&L — gross, fees, net, take rate |
| `mart_addon_revenue_monthly` | Month × addon type | Monthly roll-up for Finance reporting |
| `mart_data_quality_alerts` | Flagged issue | Every known data problem with severity and description |

---

## Requirements

- Python 3.8+
- dbt-duckdb

Install dbt and the DuckDB adapter:

```bash
pip install dbt-duckdb
```

---

## Setup

### 1. Clone the repo

```bash
git clone <repo-url>
cd stripe_revenue
```

### 2. Configure your profile

A `profiles.yml` is included in the project root. It points dbt at a local DuckDB file (`stripe_revenue.duckdb`) that gets created automatically on first run.

You have two options:

**Option A — use the included profiles.yml (simplest)**

Pass `--profiles-dir .` to every dbt command (shown in the commands below).

**Option B — copy to your dbt home directory**

```bash
cp profiles.yml ~/.dbt/profiles.yml
```

Then drop `--profiles-dir .` from the commands below.

### 3. Install dbt packages

```bash
dbt deps --profiles-dir .
```

### 4. Load the raw data

The three source tables are stored as CSV seeds. Load them into DuckDB:

```bash
dbt seed --profiles-dir .
```

### 5. Run all models

```bash
dbt run --profiles-dir .
```

### 6. Run tests (optional)

```bash
dbt test --profiles-dir .
```

---

## Querying the results

After `dbt run`, open the DuckDB file with Python:

```python
import duckdb

con = duckdb.connect("stripe_revenue.duckdb")

# Monthly revenue by addon type
con.execute("SELECT * FROM marts.mart_addon_revenue_monthly ORDER BY event_month").df()

# Charge-level P&L
con.execute("SELECT * FROM marts.mart_addon_revenue_by_charge ORDER BY charged_at").df()

# Data quality issues
con.execute("SELECT * FROM marts.mart_data_quality_alerts").df()
```

Or install the DuckDB CLI and query directly:

```bash
pip install duckdb-cli
duckdb stripe_revenue.duckdb
```

```sql
SELECT * FROM marts.mart_addon_revenue_monthly ORDER BY event_month;
```

---

## Known data issues

All issues are surfaced in `mart_dq_summary`. A summary:

| Charge | Issue |
|---|---|
| `ch_CCC` / `txn_008` | CAD charge with null original amount — USD conversion may be inaccurate |
| `ch_DDD` | Single charge maps to both `payments` and `screening` — per-addon attribution is ambiguous |
| `ch_EEE` | Charge collected but no transfer event yet — net revenue is overstated until transfer lands |
| `ch_FFF`, `ch_GGG` | Activity events (dispute fee, refund) with no matching charge record |
