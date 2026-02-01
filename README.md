# Snowflake FinOps Cost & Usage Analytics (End-to-End)

This project demonstrates a realistic FinOps pipeline in Snowflake:
- Generate synthetic cloud cost & usage export files (CUR-like)
- Land them into an internal stage
- Ingest with Snowpipe (PIPE + REFRESH)
- Store RAW semi-structured data in VARIANT
- Transform into CURATED analytics tables
- Run FinOps dashboards + Cortex AI summarization/classification for anomalies

## Why this matters
Cloud cost data is:
- High volume, append-only, and partitioned by date
- Used by Finance, Engineering, and Leadership
- Often needs governed access to sensitive info (account/project/owner mappings)

Snowflake is a strong fit for long retention cost analytics because storage scales cheaply,
compute is elastic, and you can query both raw JSON and curated models.

## Architecture
1. **Generate synthetic daily exports** (like AWS CUR / Azure cost exports)
2. **Write JSON files to internal stage**: `@RAW.COST_STAGE/exports/dt=YYYY-MM-DD/`
3. **Snowpipe ingestion**: Stage → RAW table
4. **Transform**: RAW VARIANT → CURATED fact table + dimensions
5. **Analytics**: Daily spend, service spend, team spend, anomaly detection
6. **Cortex AI**: Summarize anomaly days and classify likely cause

## How to run (Snowflake trial)
Run the scripts in order from `sql/`:
1) `00_setup.sql`
2) `01_generate_and_stage_daily_exports.sql`
3) `02_snowpipe_ingest_raw.sql`
4) `03_transform_curated.sql`
5) `04_analytics.sql`
6) `05_ai_anomaly_insights.sql`

Optional cleanup:
- `99_cleanup.sql`

## Datasets (synthetic)
The generator creates:
- ~60 days of daily exports
- Multiple accounts, services, regions, and tags (team/env/app)
- Built-in "anomaly injections" (spend spike days) so the anomaly detection has something real to find

## What this showcases
- Internal stages + JSON file formats
- Snowpipe for file-based ingestion
- VARIANT + JSON parsing / schema-on-read
- Curated dimensional model for BI
- Practical FinOps analytics queries
- Cortex AI functions (AI_SUMMARIZE_AGG, AI_CLASSIFY) for anomaly explanation

## Highlights
* **Snowpipe ingestion from daily exports** (stage partitioning, COPY INTO, pipe refresh pattern)
* **RAW VARIANT layer** for fidelity + traceability (filename/row metadata)
* **Curated dimensional model** for BI (fact + dims)
* **Anomaly detection** using window functions and trailing averages
* **Cortex AI** to explain anomalies and classify likely root cause categories
