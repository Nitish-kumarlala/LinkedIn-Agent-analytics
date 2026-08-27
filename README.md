[README (1).md](https://github.com/user-attachments/files/31524619/README.1.md)
# LinkedIn Agent Analytics

An end-to-end analytics platform that ingests LinkedIn outbound-agent activity, models it into a star schema warehouse, scores every agent for shadow-ban risk with a statistical anomaly engine, validates data quality on every run, and surfaces everything through a Power BI semantic layer.

Built as a mini "enterprise data stack" — a real-world-style pipeline (extract → validate → load → score → report) instead of a single notebook.

## Table of Contents
- [Architecture](#architecture)
- [Key Features](#key-features)
- [Tech Stack](#tech-stack)
- [Data Model](#data-model)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Running the Pipeline](#running-the-pipeline)
- [Data Quality Framework](#data-quality-framework)
- [Risk & Anomaly Engine](#risk--anomaly-engine)
- [Power BI Dashboards](#power-bi-dashboards)
- [CI/CD](#cicd)
- [Roadmap](#roadmap)

## Architecture

```
 Polluxa CRM API                 SQLite Warehouse (Star Schema)             Power BI
 (LinkedIn outreach   ─────▶     dim_agent / dim_campaign /       ─────▶    Executive Overview
  activity feed)                 dim_target_icp / dim_date /                Agent Health & Capacity
        │                        fact_daily_outreach                       Risk Anomaly Engine
        │  watermark-based
        │  incremental pull
        ▼
 Ingestion Pipeline  ──▶  Dead-Letter Queue (malformed rows)
        │
        ▼
 Data Quality Engine  ──▶  dq_results_history (Completeness, Uniqueness,
        │                   Validity, Referential Integrity, Timeliness)
        ▼
 Risk / Anomaly Model  ──▶  anomaly_score, is_anomalous, recommended_daily_invites
```

**Flow:** the ingestion pipeline pulls activity since the last watermark, routes malformed records to a dead-letter queue, and idempotently upserts clean records into a dimensional model. The DQ engine then scores every load against five weighted quality dimensions, and the risk model computes a composite anomaly score per agent-day to recommend safe outreach limits. Power BI sits on top as the reporting layer.

## Key Features

- **Incremental, resumable ingestion** — watermark file tracks the last successful pull so re-runs only fetch new activity.
- **Resilient API client** — retries with exponential backoff on `429`/`5xx`, honors `Retry-After`, and falls back to a local sandbox stream if the live endpoint is unreachable (keeps the pipeline runnable without live credentials).
- **Dead-letter queue** — records missing required keys or with invalid metrics (e.g. negative counts) are quarantined as JSON instead of silently dropped or crashing the load.
- **Idempotent upserts** — `ON CONFLICT ... DO UPDATE` on every dimension and the fact table means the pipeline can be safely re-run without creating duplicates.
- **Star schema warehouse** — conformed dimensions (`dim_agent`, `dim_campaign`, `dim_target_icp`, `dim_date`) around a single grain fact table (`fact_daily_outreach`), with surrogate keys and referential integrity enforced via foreign keys.
- **Automated data quality scoring** — every run is graded on Completeness, Uniqueness, Validity, Referential Integrity, and Timeliness, rolled into a single weighted composite score against an 85% pass threshold, with full history logged to `dq_results_history`.
- **Statistical risk / anomaly engine** — z-score-based deviation detection on acceptance rate, blended with reply-rate and acceptance-rate penalties into a 0–100 composite risk score, used to automatically recommend a throttled daily invite ceiling for at-risk agents.
- **Power BI semantic layer** — a documented DAX measure library and a three-page report (Executive Overview, Agent Health & Capacity, Risk Anomaly Engine).
- **CI pipeline** — GitHub Actions lints and compiles the codebase on every push/PR.

## Tech Stack

| Layer | Tool |
|---|---|
| Language | Python 3.11 |
| Warehouse | SQLite (star schema) |
| Ingestion | `requests`, watermark-based incremental pulls |
| Data processing | `pandas`, `numpy` |
| BI / Reporting | Power BI (`.pbix`), DAX |
| CI | GitHub Actions |

## Data Model

Star schema with one fact table at the **agent × campaign × ICP × day** grain:

| Table | Type | Purpose |
|---|---|---|
| `dim_agent` | Dimension | Agent identity, account age tier, risk classification, invite/message limits |
| `dim_campaign` | Dimension | Campaign identity and objective |
| `dim_target_icp` | Dimension | Ideal Customer Profile — job title, industry, region |
| `dim_date` | Dimension | Standard calendar attributes (day of week, weekend flag, month, quarter, year) |
| `fact_daily_outreach` | Fact | Daily invites/messages/replies/leads, plus computed `anomaly_score` and `is_anomalous` |
| `pipeline_run_metadata` | Operational | Run-level audit log — status, rows extracted/inserted/updated, errors |
| `dq_results_history` | Operational | Per-run data quality scores by dimension |

Full DDL: [`sql/ddl_star_schema.sql`](sql/ddl_star_schema.sql)

## Project Structure

```
├── .github/workflows/ci.yml       # Lint + compile check on push/PR
├── PowerBI/
│   ├── LinkedIn_analytics.pbix    # Power BI report
│   ├── LinkedIn_analytics.pdf     # Exported report
│   └── screenshots/               # Dashboard preview images
├── docs/
│   ├── docs_measures.md           # DAX measure library
│   └── part1_evidence/            # Source-system access walkthrough
├── sql/
│   └── ddl_star_schema.sql        # Warehouse schema (DDL)
└── src/
    ├── database/db_manager.py     # Schema init + dim_date population
    ├── ingestion/pipeline.py      # Extract → validate → load
    ├── modeling/anamoly_detector.py  # Statistical risk scoring
    └── quality/dq_engine.py       # Data quality checks
```

## Getting Started

**Prerequisites:** Python 3.11+

```bash
git clone https://github.com/Nitish-kumarlala/LinkedIn-Agent-analytics.git
cd LinkedIn-Agent-analytics
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install pandas numpy requests python-dotenv
```

Optionally, create a `.env` file to point at a live source and pick a database location:

```env
DATABASE_URL=sqlite:///./data/linkedin_analytics.db
POLLUXA_BASE_URL=https://sales.polluxa.com/api/v1
POLLUXA_API_KEY=your_api_key_here
LOG_LEVEL=INFO
```

> No `.env`? The pipeline still runs — it falls back to a built-in sandbox data stream so you can try the full flow end to end without live credentials.

## Running the Pipeline

Run each stage in order:

```bash
# 1. Create the star schema + populate the date dimension
python src/database/db_manager.py

# 2. Ingest activity (incremental, watermark-based)
python src/ingestion/pipeline.py

# 3. Score data quality for the latest load
python src/quality/dq_engine.py

# 4. Compute risk / anomaly scores and capacity recommendations
python src/modeling/anamoly_detector.py
```

Each script is independently runnable and prints a summary on completion.

## Data Quality Framework

Every load is graded across five weighted dimensions:

| Dimension | Weight | What it checks |
|---|---|---|
| Completeness | 25% | No nulls in required fact keys |
| Uniqueness | 25% | No duplicate rows at the fact grain |
| Validity | 20% | No negative metric values |
| Referential Integrity | 15% | Every fact row resolves to a valid dimension row |
| Timeliness | 15% | Fact table has current-period data |

Scores are combined into a single composite score; runs scoring **≥ 85%** pass. Every run's results are persisted to `dq_results_history` for trend tracking.

## Risk & Anomaly Engine

The engine estimates how likely an agent is to trigger LinkedIn's automation/spam detection:

1. Computes `acceptance_rate` (invites accepted / sent) and `reply_rate` (replies / messages sent).
2. Calculates a z-score for acceptance rate against the agent population to flag sharp deviations.
3. Combines both rates plus the z-score deviation into a composite **0–100 anomaly score**.
4. Flags any agent-day scoring **≥ 70** as anomalous.
5. Automatically recommends a **50% throttled invite ceiling** for anomalous agents to reduce shadow-ban risk.

## Power BI Dashboards

| Page | Focus |
|---|---|
| **Executive Overview** | Funnel from invites → acceptances → replies → qualified leads, campaign-level rollups, acceptance/reply/conversion rates |
| **Agent Health & Capacity Monitoring** | Per-agent invite/message capacity utilization vs. configured limits, daily volume trends |
| **Risk Anomaly Engine** | Average anomaly score, anomalous record count, risk score by day, outreach volume by risk tier |

Previews are in [`PowerBI/screenshots`](PowerBI/screenshots); the full report is [`PowerBI/LinkedIn_analytics.pbix`](PowerBI/LinkedIn_analytics.pbix) (open with Power BI Desktop) with a static export at [`PowerBI/LinkedIn_analytics.pdf`](PowerBI/LinkedIn_analytics.pdf). The complete DAX measure library is documented in [`docs/docs_measures.md`](docs/docs_measures.md).

## CI/CD

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs on every push/PR to `main`:
- Installs dependencies from `requirements.txt` (if present)
- Lints for critical syntax errors (`flake8`, `E9,F63,F7,F82`)
- Compiles the `src/` package to catch import/syntax issues before merge


