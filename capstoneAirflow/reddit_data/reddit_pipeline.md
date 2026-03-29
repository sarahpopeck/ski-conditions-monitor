# Reddit Data Pipeline (Airflow DAG)

## Overview

This pipeline ingests Reddit data using the Apify Reddit Scraper, labels each post/comment using an LLM, and aggregates daily sentiment signals per ski resort.

The goal is to create a **weak signal layer** that complements structured weather and resort data for ski condition recommendations.

---

## Objective

Reddit data is used to:

- Capture **real-world skier sentiment**
- Validate model predictions using **human observations**
- Provide **additional context** when making ski trip decisions

This is NOT the primary signal — it acts as:
> a **confirmation / override layer** on top of weather + resort data

---

## Pipeline Architecture

```
Apify (Reddit API)
        ↓
Extract JSON (Airflow)
        ↓
Normalize + Deduplicate
        ↓
LLM Labeling (Mistral via LangChain)
        ↓
Postgres Load (2 tables)
        ↓
Aggregated Daily Signals
```

---

## DAG Schedule

- Runs: **Once daily at 7 PM**
- Data window: **Last 7 days of Reddit posts**
- Subreddit: `r/icecoast`

---

## Data Extraction

Source: Apify Reddit Scraper

### Queries
- "loon conditions"
- "sugarloaf conditions"
- "killington conditions"
- "pico conditions"
- "stratton conditions"
- "sugarbush conditions"
- "sunday river conditions"

### Filters
- subreddit = `icecoast`
- timeframe = `week`
- maxPostsPerQuery = 100
- comments included

---

## Deduplication Strategy

Because Apify does not provide incremental extraction:

- Data is re-pulled every run (last 7 days)
- Deduplication is handled using:

```
source_item_key = post_id + "_" + comment_id
```

### Database constraint
```sql
PRIMARY KEY (source_item_key)
```

### Insert strategy
```sql
ON CONFLICT DO NOTHING
```

---

## LLM Labeling

Each Reddit item is labeled using Mistral via LangChain.

### Inputs
- title
- text
- created_utc_iso

### Outputs

| Field | Description |
|------|------------|
| report_type | `field_report`, `forecast_opinion`, `irrelevant` |
| resort | standardized resort name |
| resolved_date | ski date referenced |
| date_resolution_source | `explicit`, `relative`, `fallback_created` |
| sentiment_label | `positive`, `negative`, `neutral` |

---

## Date Resolution Logic

Priority order:

1. **Explicit dates**
   - “March 25”, “3/25”

2. **Relative terms**
   - “today”, “tomorrow”, “this weekend”

3. **Fallback**
   - uses `created_utc_iso`

---

## Database Tables

### 1. reddit_labeled

Stores individual labeled Reddit items for auditability.

```sql
source_item_key TEXT PRIMARY KEY,
post_id TEXT,
comment_id TEXT,
record_type TEXT,
subreddit TEXT,
author TEXT,
title TEXT,
text TEXT,
permalink TEXT,
url TEXT,
created_utc_iso TIMESTAMPTZ,
scraped_at_iso TIMESTAMPTZ,
report_type TEXT,
resort TEXT,
resolved_date DATE,
date_resolution_source TEXT,
sentiment_label TEXT,
labeling_model TEXT,
labeled_at TIMESTAMPTZ
```

**Purpose:**
- Full audit trail
- Debug labeling quality
- Future model improvements

---

### 2. reddit_resort_day_signals

Aggregated daily sentiment per resort.

```sql
PRIMARY KEY (resort, ski_date)
```

| Column | Description |
|------|------------|
| field_report_count | Number of real condition reports |
| forecast_opinion_count | Number of future opinions |
| positive_count | Positive sentiment count |
| negative_count | Negative sentiment count |
| neutral_count | Neutral sentiment count |
| net_sentiment | positive - negative |
| weighted_signal_score | Weighted sentiment score |
| signal_present | Binary flag |
| updated_at | Last update timestamp |

---

## Aggregation Logic

Aggregation is performed per:

```
(resort, ski_date)
```

Only includes:
- rows with `resolved_date IS NOT NULL`
- relevant `report_type` values

### Example

```
Killington | 2026-03-28
→ 5 posts
→ 3 positive, 2 negative
→ net_sentiment = +1
```

---

## Signal Weighting

Example weighting logic:

- Field reports → higher weight
- Forecast opinions → lower weight

```
weighted_signal_score =
    (field_report_weight * field_sentiment)
  + (forecast_weight * forecast_sentiment)
```

---

## Known Limitations

- Sparse data (not all resorts/dates have Reddit coverage)
- LLM misclassification risk
- Date resolution ambiguity
- Reddit bias (active users ≠ full population)

---

## Validation Strategy

Reddit signals are used to:

- Compare against:
  - Open-Meteo forecasts
  - Resort API conditions
- Validate:
  - whether predicted conditions match real reports

---

## Future Improvements

- Async LLM labeling (faster + cheaper)
- Confidence scoring per label
- Thread-aware context (parent-child relationships)
- Multi-subreddit ingestion
- Hybrid rule-based + LLM labeling

---

## Secrets Handling

- API keys stored in `.env`
- Mounted into Docker container
- Loaded using:

```python
load_dotenv(Path(__file__).parent / ".env")
```

---

## Pipeline Guarantees

- Idempotent loads (`ON CONFLICT DO NOTHING`)
- Safe reruns
- Deduplicated inputs
- Auditable outputs

---

## Big Picture

```
Human Signal Layer (Reddit)
        +
Weather Data
        +
Resort Data
        =
Final Recommendation Engine
```
