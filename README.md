# DataForge

[![PyPI version](https://badge.fury.io/py/dataforge-dfg.svg)](https://badge.fury.io/py/dataforge-dfg)
[![Python](https://img.shields.io/badge/Python-3.11%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Status](https://img.shields.io/badge/Status-Beta-orange)]()

**DataForge** is a lightweight, pure-Python ELT framework designed for data engineers who want the power of SQL-based transformations and Python-based ingestion in a single, dependency-minimal tool — without the overhead of heavy frameworks.

It combines declarative SQL modeling (inspired by dbt) with programmable data ingestion (inspired by dlt), delivering a complete ELT pipeline from a single command-line interface.

---

## Overview

DataForge organizes data pipelines around two building blocks:

- **Python models** — extract data from any source (APIs, files, databases) and load it into your target warehouse
- **SQL models** — transform raw data into analytics-ready tables and views using Jinja2-templated SQL

Both run in a single DAG with automatic dependency resolution, parallel execution, and full observability.

---

## Installation

```bash
pip install dataforge-dfg
```

DataForge requires Python 3.11 or higher. The only mandatory dependencies are `jinja2` and `pyyaml`. You are responsible for installing the database driver of your choice:

```bash
pip install duckdb          # DuckDB
pip install psycopg2-binary # PostgreSQL
pip install mysql-connector-python # MySQL
# SQLite is included in the Python standard library
```

---

## Quickstart

```bash
mkdir my_project && cd my_project
dfg init
dfg run
```

`dfg init` detects which database drivers are installed in your environment and guides you through the initial configuration. The result is a ready-to-run project with example models and a structured directory layout.

---

## Core Concepts

### SQL Models

SQL files in `models/` are compiled through Jinja2 before execution. Dependencies between models are declared with `ref()` and resolved automatically into a DAG.

```sql
-- models/stg_orders.sql
{{ config(materialized='table') }}

SELECT
    id          AS order_id,
    customer_id,
    total_amount,
    status,
    created_at
FROM raw_orders
WHERE status != 'cancelled'
```

```sql
-- models/fct_revenue.sql
{{ config(materialized='table') }}

SELECT
    DATE(created_at) AS date,
    SUM(total_amount) AS revenue
FROM {{ ref('stg_orders') }}
GROUP BY DATE(created_at)
```

Supported materializations: `table`, `view`, `incremental`.

### Python Models

Python files in `models/` handle the extraction and loading phase. Any function named `model(context)` that returns a list of dictionaries is a valid model. Schema creation and evolution are handled automatically.

```python
# models/ingest_customers.py
DEPENDENCIES = []

def model(context):
    import urllib.request, json
    with urllib.request.urlopen("https://api.example.com/customers") as r:
        return json.loads(r.read())["data"]
```

The `context` object provides access to project configuration, upstream model data via `ref()`, and a persistent state mechanism for incremental ingestion.

### Data Contracts

Column-level tests are declared in `schema.yml` and validated with `dfg test`:

```yaml
version: 1
models:
  - name: stg_orders
    columns:
      - name: order_id
        tests:
          - not_null
          - unique
```

### Snapshots — SCD Type 2

Snapshots track historical changes to records over time using the Slowly Changing Dimensions Type 2 pattern. Control columns (`dfg_valid_from`, `dfg_valid_to`, `dfg_is_active`) are added automatically.

```sql
{% snapshot snapshot_customers %}
{{ config(unique_key='id', updated_at='updated_at') }}
SELECT * FROM {{ ref('stg_customers') }}
{% endsnapshot %}
```

---

## Command Reference

| Command | Description |
|---|---|
| `dfg init` | Initialize a new project in the current directory |
| `dfg run` | Execute the full pipeline (ingestion + transformation) |
| `dfg ingest` | Run Python models only |
| `dfg transform` | Run SQL models only |
| `dfg test` | Validate data contracts |
| `dfg compile` | Compile Jinja2 templates and generate manifest (dry run) |
| `dfg seed` | Load static CSV files into the database |
| `dfg snapshot` | Run SCD Type 2 snapshots |
| `dfg docs [--serve]` | Generate HTML lineage graph |
| `dfg debug` | Diagnose environment and database connectivity |
| `dfg log <ID>` | Search the daily log by session ID |

---

## Project Structure

```
my_project/
├── dfg_project.toml    # Project configuration
├── profiles.toml       # Database credentials (do not commit)
├── models/             # SQL and Python models
│   └── schema.yml      # Data contracts
├── snapshots/          # SCD Type 2 snapshot definitions
├── seeds/              # Static CSV files
└── target/             # Generated artifacts (do not commit)
    ├── manifest.json
    └── run_results.json
```

---

## Supported Databases

DataForge works with any database that implements the Python DB-API 2.0 (PEP 249) specification. Tested adapters:

| Database | Driver |
|---|---|
| DuckDB | `duckdb` |
| PostgreSQL | `psycopg2` |
| MySQL | `mysql-connector-python` |
| SQLite | built-in |

---

## License

MIT License. See [LICENSE](LICENSE) for details.