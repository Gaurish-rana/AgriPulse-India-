-- ============================================================================
-- AgriPulse India — Phase 3: Snowflake Database Setup
-- File: snowflake/sql/01_database_setup.sql
-- Description: Creates Database, Warehouses, Roles, Schemas, Stages & Formats
-- ============================================================================

-- Use appropriate role (SYSADMIN or ACCOUNTADMIN)
USE ROLE ACCOUNTADMIN;

-- 1. Create Dedicated Virtual Warehouse
CREATE WAREHOUSE IF NOT EXISTS AGRIPULSE_WH
    WITH 
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Virtual warehouse for AgriPulse India batch and streaming workloads';

USE WAREHOUSE AGRIPULSE_WH;

-- 2. Create Central Database
CREATE DATABASE IF NOT EXISTS AGRIPULSE_DB
    COMMENT = 'AgriPulse India centralized agricultural big data warehouse';

USE DATABASE AGRIPULSE_DB;

-- 3. Create Schemas
-- RAW: Ingestion-level staging tables and audit logs
CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Landing zone for raw batch files and raw streaming payloads';

-- CLEAN: Standardized, validated, and deduplicated datasets
CREATE SCHEMA IF NOT EXISTS CLEAN
    COMMENT = 'Conformed and cleansed datasets with standardized naming and data types';

-- MART: Analytical dimensional models (Star Schema)
CREATE SCHEMA IF NOT EXISTS MART
    COMMENT = 'Dimensional business intelligence marts, facts, and dimensions';

-- 4. Create File Formats in RAW & CLEAN
USE SCHEMA RAW;

CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
    TYPE = 'PARQUET'
    COMPRESSION = 'AUTO'
    COMMENT = 'Parquet file format for loading processed datasets';

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL', 'null', '\\N')
    COMMENT = 'Standard CSV format with header skipping';

-- 5. Create Internal Stages for Data Staging
CREATE STAGE IF NOT EXISTS AGRIPULSE_STAGE
    FILE_FORMAT = PARQUET_FORMAT
    COMMENT = 'Internal stage for staging Parquet data files via PUT';

CREATE STAGE IF NOT EXISTS AGRIPULSE_CSV_STAGE
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'Internal stage for raw CSV loading';

-- Show setup summary
SHOW SCHEMAS IN DATABASE AGRIPULSE_DB;
