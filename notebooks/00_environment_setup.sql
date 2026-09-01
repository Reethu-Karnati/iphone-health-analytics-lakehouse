-- Databricks notebook source
SELECT
    current_catalog() AS current_catalog,
    current_schema() AS current_schema;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS iphone_health_bronze
COMMENT 'Raw and minimally processed Apple Health records';

CREATE SCHEMA IF NOT EXISTS iphone_health_silver
COMMENT 'Validated, standardized, deduplicated, and privacy-filtered health records';

CREATE SCHEMA IF NOT EXISTS iphone_health_gold
COMMENT 'Analytics-ready health metrics, features, and aggregates';

CREATE SCHEMA IF NOT EXISTS iphone_health_quarantine
COMMENT 'Records that fail parsing or data-quality expectations';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS iphone_health_bronze.landing
COMMENT 'Landing location for protected Apple Health source files';

-- COMMAND ----------

SHOW SCHEMAS;

SHOW VOLUMES IN iphone_health_bronze;

-- COMMAND ----------

