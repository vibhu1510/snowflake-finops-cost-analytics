/* ============================================================
   00_setup.sql — Core setup for FinOps pipeline
   ============================================================ */

-- Optional: USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE WAREHOUSE FINOPS_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

CREATE OR REPLACE DATABASE FINOPS_LAB;
CREATE OR REPLACE SCHEMA FINOPS_LAB.RAW;
CREATE OR REPLACE SCHEMA FINOPS_LAB.CURATED;

USE WAREHOUSE FINOPS_WH;
USE DATABASE FINOPS_LAB;

-- Internal stage for daily export files
CREATE OR REPLACE STAGE RAW.COST_STAGE;

-- JSON parsing config
CREATE OR REPLACE FILE FORMAT RAW.JSON_FF
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE;

-- A place to track "last transformed date" if you want incremental logic later
CREATE OR REPLACE TABLE RAW.PIPELINE_STATE (
  key STRING,
  value STRING
);

MERGE INTO RAW.PIPELINE_STATE t
USING (SELECT 'last_transform_dt' AS key, NULL::STRING AS value) s
ON t.key = s.key
WHEN NOT MATCHED THEN INSERT (key, value) VALUES (s.key, s.value);
