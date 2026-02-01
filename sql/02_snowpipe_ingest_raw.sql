/* ============================================================
   02_snowpipe_ingest_raw.sql
   - Create RAW landing table (VARIANT)
   - Create Snowpipe (pipe)
   - Refresh pipe to ingest from internal stage
   ============================================================ */

USE WAREHOUSE FINOPS_WH;
USE DATABASE FINOPS_LAB;
USE SCHEMA RAW;

-- 1) RAW landing table (schema-on-read)
CREATE OR REPLACE TABLE RAW.COST_RAW (
  ingest_ts  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  filename   STRING,
  row_number NUMBER,
  raw        VARIANT
);

-- 2) Snowpipe definition (COPY INTO wrapped in a pipe)
CREATE OR REPLACE PIPE RAW.COST_PIPE AS
COPY INTO RAW.COST_RAW (filename, row_number, raw)
FROM (
  SELECT
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER,
    $1
  FROM @RAW.COST_STAGE/exports/
)
FILE_FORMAT = RAW.JSON_FF;

-- 3) For internal stage demo, use REFRESH (external stage would use auto-ingest notifications)
ALTER PIPE RAW.COST_PIPE REFRESH;

-- 4) Pipe health check + validation
SELECT SYSTEM$PIPE_STATUS('RAW.COST_PIPE');

SELECT COUNT(*) AS raw_rows FROM RAW.COST_RAW;

-- Quick peek
SELECT filename, row_number, raw
FROM RAW.COST_RAW
QUALIFY ROW_NUMBER() OVER (ORDER BY ingest_ts DESC) <= 5;
