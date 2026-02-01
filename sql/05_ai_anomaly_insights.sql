/* ============================================================
   05_ai_anomaly_insights.sql — Cortex AI insights for anomalies
   - AI_SUMMARIZE_AGG: summarize an anomaly day in plain English
   - AI_CLASSIFY: label likely cause category
   ============================================================ */

USE WAREHOUSE FINOPS_WH;
USE DATABASE FINOPS_LAB;
USE SCHEMA CURATED;

-- 1) Build an "anomaly days" list (same logic as analytics)
CREATE OR REPLACE TEMP TABLE TMP_ANOMALY_DAYS AS
WITH daily AS (
  SELECT usage_date, SUM(cost_usd) AS total_cost
  FROM CURATED.FACT_COST_DAILY
  GROUP BY 1
),
scored AS (
  SELECT
    usage_date,
    total_cost,
    AVG(total_cost) OVER (ORDER BY usage_date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS trailing_avg_7d,
    CASE
      WHEN trailing_avg_7d IS NULL THEN NULL
      ELSE total_cost / NULLIF(trailing_avg_7d, 0)
    END AS spike_ratio
  FROM daily
)
SELECT usage_date, total_cost, trailing_avg_7d, spike_ratio
FROM scored
WHERE spike_ratio >= 1.70;

-- 2) Pick the biggest anomaly day
CREATE OR REPLACE TEMP TABLE TMP_TOP_ANOMALY AS
SELECT *
FROM TMP_ANOMALY_DAYS
ORDER BY spike_ratio DESC, usage_date DESC
LIMIT 1;

SELECT * FROM TMP_TOP_ANOMALY;

-- 3) AI_SUMMARIZE_AGG: Summarize what changed on that anomaly day
--    This is great for a 1-minute demo moment.
WITH top_day AS (SELECT usage_date AS dt FROM TMP_TOP_ANOMALY)
SELECT
  AI_SUMMARIZE_AGG(
    CONCAT(
      'date=', TO_VARCHAR(f.usage_date),
      ' vendor=', f.cloud_vendor,
      ' acct=', f.account_id,
      ' service=', f.service,
      ' region=', f.region,
      ' usage_type=', f.usage_type,
      ' team=', f.team,
      ' env=', f.env,
      ' app=', f.app,
      ' cost_usd=', TO_VARCHAR(ROUND(f.cost_usd, 2))
    )
  ) AS anomaly_day_summary
FROM CURATED.FACT_COST_DAILY f
JOIN top_day d ON f.usage_date = d.dt;

-- 4) AI_CLASSIFY: classify likely cause category
--    Labels are FinOps-friendly, not security-friendly.
WITH top_day AS (SELECT usage_date AS dt FROM TMP_TOP_ANOMALY),
rollup AS (
  SELECT
    f.usage_date,
    f.service,
    f.team,
    f.env,
    f.app,
    ROUND(SUM(f.cost_usd), 2) AS cost_usd
  FROM CURATED.FACT_COST_DAILY f
  JOIN top_day d ON f.usage_date = d.dt
  GROUP BY 1,2,3,4,5
),
top_contrib AS (
  SELECT *
  FROM rollup
  QUALIFY ROW_NUMBER() OVER (ORDER BY cost_usd DESC) <= 25
)
SELECT
  AI_CLASSIFY(
    AI_SUMMARIZE_AGG(
      CONCAT(
        'service=', service,
        ' team=', team,
        ' env=', env,
        ' app=', app,
        ' cost_usd=', TO_VARCHAR(cost_usd)
      )
    ),
    ['new_deployment_or_scale_up','logging_or_observability_spike','data_egress_or_network_spike','storage_growth','misconfiguration','seasonal_or_expected_increase','unknown']
  ) AS likely_cause_labels;
