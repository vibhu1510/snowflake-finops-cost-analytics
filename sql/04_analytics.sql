/* ============================================================
   04_analytics.sql — FinOps analytics queries
   ============================================================ */

USE WAREHOUSE FINOPS_WH;
USE DATABASE FINOPS_LAB;
USE SCHEMA CURATED;

-- 1) Total spend by day
SELECT
  usage_date,
  ROUND(SUM(cost_usd), 2) AS total_cost_usd
FROM CURATED.FACT_COST_DAILY
GROUP BY 1
ORDER BY 1;

-- 2) Spend by service (overall)
SELECT
  service,
  ROUND(SUM(cost_usd), 2) AS cost_usd
FROM CURATED.FACT_COST_DAILY
GROUP BY 1
ORDER BY cost_usd DESC;

-- 3) Spend by team (overall) - focus on prod
SELECT
  team,
  ROUND(SUM(cost_usd), 2) AS cost_usd
FROM CURATED.FACT_COST_DAILY
WHERE env='prod'
GROUP BY 1
ORDER BY cost_usd DESC;

-- 4) Top services by cost for a specific day (most recent)
WITH last_day AS (
  SELECT MAX(usage_date) AS dt FROM CURATED.FACT_COST_DAILY
)
SELECT
  f.service,
  ROUND(SUM(f.cost_usd), 2) AS cost_usd
FROM CURATED.FACT_COST_DAILY f
JOIN last_day d ON f.usage_date = d.dt
GROUP BY 1
ORDER BY cost_usd DESC;

-- 5) Simple anomaly detection (day-over-day spike)
--    Flag days where total spend is > 1.7x of the 7-day trailing average.
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
SELECT
  usage_date,
  ROUND(total_cost, 2) AS total_cost_usd,
  ROUND(trailing_avg_7d, 2) AS trailing_avg_7d_usd,
  ROUND(spike_ratio, 2) AS spike_ratio
FROM scored
WHERE spike_ratio >= 1.70
ORDER BY spike_ratio DESC, usage_date DESC;

-- 6) Drill-down: for a given anomaly day, show what caused it (service/team)
--    Replace '2026-01-15' with a date returned by query #5.
SELECT
  service,
  team,
  ROUND(SUM(cost_usd), 2) AS cost_usd
FROM CURATED.FACT_COST_DAILY
WHERE usage_date = TO_DATE('2026-01-15')
GROUP BY 1,2
ORDER BY cost_usd DESC;
