/* ============================================================
   03_transform_curated.sql
   - Create curated dimensional model
   - Transform RAW VARIANT into curated tables
   ============================================================ */

USE WAREHOUSE FINOPS_WH;
USE DATABASE FINOPS_LAB;

-- CURATED layer
USE SCHEMA CURATED;

-- Dimension tables
CREATE OR REPLACE TABLE CURATED.DIM_ACCOUNT (
  account_id   STRING,
  cloud_vendor STRING
);

CREATE OR REPLACE TABLE CURATED.DIM_SERVICE (
  service STRING
);

CREATE OR REPLACE TABLE CURATED.DIM_TAGS (
  team STRING,
  env  STRING,
  app  STRING
);

-- Fact table
CREATE OR REPLACE TABLE CURATED.FACT_COST_DAILY (
  usage_date   DATE,
  account_id   STRING,
  cloud_vendor STRING,
  service      STRING,
  region       STRING,
  usage_type   STRING,
  usage_qty    NUMBER(18,4),
  unit         STRING,
  cost_usd     NUMBER(18,6),
  currency     STRING,
  team         STRING,
  env          STRING,
  app          STRING,
  filename     STRING
);

-- Load curated from RAW
INSERT INTO CURATED.FACT_COST_DAILY
SELECT
  TRY_TO_DATE(r.raw:usage_date::string)                 AS usage_date,
  r.raw:account_id::string                              AS account_id,
  r.raw:cloud_vendor::string                            AS cloud_vendor,
  r.raw:service::string                                 AS service,
  r.raw:region::string                                  AS region,
  r.raw:usage_type::string                              AS usage_type,
  r.raw:usage_qty::number                               AS usage_qty,
  r.raw:unit::string                                    AS unit,
  r.raw:cost_usd::number                                AS cost_usd,
  r.raw:currency::string                                AS currency,
  r.raw:tags:team::string                               AS team,
  r.raw:tags:env::string                                AS env,
  r.raw:tags:app::string                                AS app,
  r.filename                                            AS filename
FROM FINOPS_LAB.RAW.COST_RAW r;

-- Populate dimensions (simple distinct loads for demo)
INSERT OVERWRITE INTO CURATED.DIM_ACCOUNT
SELECT DISTINCT account_id, cloud_vendor
FROM CURATED.FACT_COST_DAILY;

INSERT OVERWRITE INTO CURATED.DIM_SERVICE
SELECT DISTINCT service
FROM CURATED.FACT_COST_DAILY;

INSERT OVERWRITE INTO CURATED.DIM_TAGS
SELECT DISTINCT team, env, app
FROM CURATED.FACT_COST_DAILY;

-- Validation
SELECT COUNT(*) AS fact_rows FROM CURATED.FACT_COST_DAILY;
SELECT MIN(usage_date) AS min_dt, MAX(usage_date) AS max_dt FROM CURATED.FACT_COST_DAILY;
