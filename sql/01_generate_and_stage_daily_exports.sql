/* ============================================================
   01_generate_and_stage_daily_exports.sql
   - Generate synthetic cost & usage rows
   - Convert to CUR-like JSON objects
   - Export JSON files to internal stage partitioned by day
   ============================================================ */

USE WAREHOUSE FINOPS_WH;
USE DATABASE FINOPS_LAB;
USE SCHEMA RAW;

-- 1) Source generator table (structured)
CREATE OR REPLACE TABLE RAW.COST_SYNTH_SRC (
  usage_date      DATE,
  account_id      STRING,
  cloud_vendor    STRING,
  service         STRING,
  region          STRING,
  resource_id     STRING,
  usage_type      STRING,
  usage_qty       NUMBER(18,4),
  unit            STRING,
  cost_usd        NUMBER(18,6),
  currency        STRING,
  team            STRING,
  env             STRING,
  app             STRING
);

-- 2) Generate ~60 days of daily costs
--    Includes deliberate anomaly injection:
--    - A few random days get a multiplier (spend spike) for a service/team.
INSERT INTO RAW.COST_SYNTH_SRC
WITH params AS (
  SELECT
    60                                         AS days_back,
    18                                         AS accounts,
    7                                          AS services,
    5                                          AS regions,
    10                                         AS teams,
    6                                          AS apps
),
base AS (
  SELECT
    DATEADD('day', -seq4(), CURRENT_DATE())::DATE AS usage_date
  FROM TABLE(GENERATOR(ROWCOUNT => (SELECT days_back FROM params)))
),
rows AS (
  SELECT
    b.usage_date,
    'acct-' || LPAD(UNIFORM(1,(SELECT accounts FROM params),RANDOM())::STRING, 3, '0') AS account_id,
    DECODE(UNIFORM(1,3,RANDOM()),1,'aws',2,'azure','gcp')                               AS cloud_vendor,
    DECODE(UNIFORM(1,(SELECT services FROM params),RANDOM()),
      1,'ec2',2,'s3',3,'rds',4,'eks',5,'cloudwatch',6,'lambda','athena')               AS service,
    DECODE(UNIFORM(1,(SELECT regions FROM params),RANDOM()),
      1,'us-east-1',2,'us-west-2',3,'eu-west-1',4,'ap-southeast-1','eu-central-1')    AS region,
    'res-' || UNIFORM(1,2000,RANDOM())                                                 AS resource_id,
    DECODE(UNIFORM(1,6,RANDOM()),
      1,'compute_hours',2,'storage_gb_month',3,'requests',4,'data_transfer_gb',5,'iops','log_ingest_gb') AS usage_type,
    ROUND(UNIFORM(1, 5000, RANDOM()) / 10.0, 4)                                        AS usage_qty,
    DECODE(UNIFORM(1,4,RANDOM()),1,'hours',2,'gb-month',3,'count','gb')                AS unit,
    NULL::NUMBER(18,6)                                                                 AS cost_usd,
    'USD'                                                                              AS currency,
    'team-' || LPAD(UNIFORM(1,(SELECT teams FROM params),RANDOM())::STRING,2,'0')      AS team,
    DECODE(UNIFORM(1,3,RANDOM()),1,'prod',2,'staging','dev')                           AS env,
    'app-' || LPAD(UNIFORM(1,(SELECT apps FROM params),RANDOM())::STRING,2,'0')        AS app
  FROM base b
  -- per-day event volume
  CROSS JOIN TABLE(GENERATOR(ROWCOUNT => 1200))
),
pricing AS (
  SELECT
    *,
    -- simple pricing model by usage_type; real CUR is more complex but this works for demos
    CASE usage_type
      WHEN 'compute_hours'       THEN usage_qty * 0.08
      WHEN 'storage_gb_month'    THEN usage_qty * 0.02
      WHEN 'requests'            THEN usage_qty * 0.000002
      WHEN 'data_transfer_gb'    THEN usage_qty * 0.09
      WHEN 'iops'                THEN usage_qty * 0.00001
      WHEN 'log_ingest_gb'       THEN usage_qty * 0.15
      ELSE usage_qty * 0.05
    END AS base_cost
  FROM rows
),
anomalies AS (
  SELECT
    p.*,
    -- inject spikes on a few dates: 3% chance of a 3x to 10x multiplier when prod + certain service
    CASE
      WHEN env='prod'
        AND service IN ('eks','cloudwatch','log_ingest_gb','ec2')  -- intentional mix
        AND UNIFORM(1,100,RANDOM()) <= 3
      THEN UNIFORM(3,10,RANDOM())
      ELSE 1
    END AS spike_mult
  FROM pricing p
)
SELECT
  usage_date, account_id, cloud_vendor, service, region, resource_id, usage_type,
  usage_qty, unit,
  ROUND(base_cost * spike_mult, 6) AS cost_usd,
  currency, team, env, app
FROM anomalies;

-- 3) View: convert each row into a JSON object (CUR-like)
CREATE OR REPLACE VIEW RAW.COST_SYNTH_JSON_V AS
SELECT
  OBJECT_CONSTRUCT(
    'usage_date', TO_VARCHAR(usage_date),
    'account_id', account_id,
    'cloud_vendor', cloud_vendor,
    'service', service,
    'region', region,
    'resource_id', resource_id,
    'usage_type', usage_type,
    'usage_qty', usage_qty,
    'unit', unit,
    'cost_usd', cost_usd,
    'currency', currency,
    'tags', OBJECT_CONSTRUCT(
      'team', team,
      'env', env,
      'app', app
    )
  ) AS cost_record
FROM RAW.COST_SYNTH_SRC;

-- 4) Export to internal stage partitioned by day (dt=YYYY-MM-DD)
--    Snowflake will create multiple files across partitions.
COPY INTO @RAW.COST_STAGE/exports/
FROM (
  SELECT
    cost_record,
    'dt=' || TO_VARCHAR(TO_DATE(cost_record:usage_date::string)) AS dt_partition
  FROM RAW.COST_SYNTH_JSON_V
)
PARTITION BY (dt_partition)
FILE_FORMAT = (TYPE=JSON)
OVERWRITE = TRUE;

-- 5) Verify exports exist
LIST @RAW.COST_STAGE/exports/;
