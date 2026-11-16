-- ============================================================================
-- AgriPulse India — Phase 3: ClickHouse Database Initialization
-- File: clickhouse/sql/01_init_databases.sql
-- Description: Creates isolated databases for RAW, CLEAN, and MART
-- ============================================================================

-- 1. Ingestion / Landing Database
CREATE DATABASE IF NOT EXISTS agripulse_raw
COMMENT 'Landing zone for raw batch files, streaming buffers, and pipeline audit logs';

-- 2. Conformed / Clean Database
CREATE DATABASE IF NOT EXISTS agripulse_clean
COMMENT 'Conformed, standardized, deduplicated datasets with strictly typed columns';

-- 3. Analytical Business Intelligence Mart Database
CREATE DATABASE IF NOT EXISTS agripulse_mart
COMMENT 'Dimensional star schema marts, optimized for analytical dashboards & OLAP queries';

SHOW DATABASES;
