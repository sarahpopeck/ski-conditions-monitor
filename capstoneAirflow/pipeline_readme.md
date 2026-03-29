# 🏔️ Ski Resort Data Pipeline (Airflow-Orchestrated)

## 📌 Overview

This project builds a data pipeline to ingest, transform, and store ski resort conditions data and weather forecasts.

The system collects data from multiple resort APIs and Open-Meteo, normalizes the data into a unified schema, and stores it in a PostgreSQL database for downstream analytics and decision modeling.

The pipeline is orchestrated using Apache Airflow and follows a modular architecture:

* **Extraction (runner.py)** → Fetch raw JSON from APIs
* **Validation** → Ensure schema consistency
* **Transformation (Parsers)** → Normalize into a unified schema
* **Loading** → Insert into Postgres

---

## 🧱 Architecture

```
            ┌──────────────┐
            │   Airflow    │
            │    DAGs      │
            └──────┬───────┘
                   │
        ┌──────────┼──────────┐
        │                     │
 ┌──────────────┐     ┌──────────────┐
 │ Ski Resorts  │     │ Open-Meteo   │
 │     DAG      │     │     DAG      │
 └──────┬───────┘     └──────┬───────┘
        │                     │
   Extract → Validate → Parse → Load
        │                     │
        └──────────┬──────────┘
                   │
           ┌──────────────┐
           │  PostgreSQL  │
           │   Database   │
           └──────────────┘
```

---

## 📂 Repository Structure

```
project/
│
├── runner.py                  # Extraction, validation, and parsing logic
├── dags/
│   ├── ski_resort_dag.py      # Resort ingestion DAG
│   └── openmeteo_dag.py       # Weather ingestion DAG
│
├── data/
│   └── raw/                   # Stored raw JSON snapshots
│
└── notebooks/                # Experimental / development (not production)
```

---

## 🔄 Data Pipeline Flow

Each resort follows this pipeline:

### 1. Extract

* Triggered via Airflow task
* Uses `runner.py`
* Fetches JSON from external APIs
* Saves raw payload to disk

```bash
python runner.py extract --job <job_id>
```

---

### 2. Validate

* Ensures payload structure is correct
* Prevents downstream parsing failures

Validation functions:

* `validate_reportpal`
* `validate_killington`
* `validate_mtnpowder`
* `validate_openmeteo`

---

### 3. Transform (Parsing)

Raw JSON is transformed into a **unified schema**:

```json
{
  "resort": str,
  "report_date": date,
  "raw_path": str,
  "resort_updated_at": str,
  "trails_open": int,
  "trails_total": int,
  "open_trails_pct": int,
  "lifts_open": int,
  "lifts_total": int,
  "open_lifts_pct": int,
  "mountain_report_text": str
}
```

---

## 🧩 Parser Families

### 1. Boyne Resorts (ReportPal)

* Resorts:

  * Loon
  * Sunday River
  * Sugarloaf
* Input: **1 JSON**
* Parser:

  * `parse_resort_family_boyne`

---

### 2. Killington Family

* Resorts:

  * Killington
  * Pico
* Input: **3 JSONs**

  * lifts
  * trails
  * snow report
* Parsers:

  * `parse_killington_lifts`
  * `parse_killington_trails`
  * `parse_killington_snow_report`
  * `parse_resort_family_killington` (final aggregation)

---

### 3. MountainPowder Resorts

* Resorts:

  * Stratton
  * Sugarbush
* Input: **1 JSON**
* Parser:

  * `parse_resort_family_mountainpowder`

---

## 🗄️ Database Design

### Table: `resort_status_daily`

Stores normalized daily resort snapshots.

**Columns:**

* resort
* report_date
* raw_path
* resort_updated_at
* trails_open
* trails_total
* open_trails_pct
* lifts_open
* lifts_total
* open_lifts_pct
* mountain_report_text

**Constraint:**

```sql
UNIQUE (resort, report_date, resort_updated_at)
```

**Insert behavior:**

* Uses `ON CONFLICT DO NOTHING`
* Prevents duplicate ingestion

---

## ⚙️ Airflow Design

### Ski Resort DAG

* One flow per resort
* Handles:

  * extract
  * validate
  * parse
  * load

### Open-Meteo DAG

* Separate pipeline for weather forecasts
* Stores:

  * hourly forecasts
  * daily forecasts

---

## 🚀 Design Principles

### 1. Separation of Concerns

* runner.py → extraction + validation + parsing
* DAGs → orchestration + loading

### 2. Unified Schema

* All resort sources normalized into one table

### 3. Source-Aware Parsing

* Different parsers per API structure
* Same output schema

### 4. Idempotency

* Duplicate inserts prevented via unique constraints

### 5. Extensibility

* New resorts = add parser + job config
* No need to change DB schema

---

## 🧪 Development vs Production

| Component | Role                    |
| --------- | ----------------------- |
| Notebooks | Prototyping & debugging |
| DAGs      | Production pipelines    |
| runner.py | Stable extraction layer |

---

## 🔜 Next Steps

* Replace outdated DAG parsing logic with new parser functions
* Standardize DB insert function for row-level ingestion
* Integrate Open-Meteo parsing into separate DAG
* Add monitoring and alerting

---

## 🧠 Key Insight

This pipeline converts **heterogeneous ski resort APIs** into a **single unified analytical dataset**, enabling:

* decision modeling (trip quality, risk)
* RAG-based summaries
* downstream ML applications

---
