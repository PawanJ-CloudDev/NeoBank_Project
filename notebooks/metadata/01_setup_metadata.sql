# Databricks notebook source
# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC -- Databricks notebook source
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- 1. DROP EXISTING METADATA TABLES
# MAGIC -- =====================================================
# MAGIC
# MAGIC DROP TABLE IF EXISTS banking.metadata.tables;
# MAGIC
# MAGIC DROP TABLE IF EXISTS banking.metadata.table_parameters;
# MAGIC
# MAGIC DROP TABLE IF EXISTS banking.metadata.table_watermarks;
# MAGIC
# MAGIC DROP TABLE IF EXISTS banking.metadata.pipeline_runs;
# MAGIC
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- DBTITLE 1,Create Metadata Tables
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- CATALOG & SCHEMA
# MAGIC -- =====================================================
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS banking;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS banking.metadata;
# MAGIC
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- 1️⃣ metadata.tables
# MAGIC -- Static registry of logical tables
# MAGIC -- =====================================================
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS banking.metadata.tables (
# MAGIC
# MAGIC     table_id        INT,
# MAGIC     table_name      STRING,
# MAGIC
# MAGIC     source_system   STRING,       -- postgresql / blob
# MAGIC     source_schema   STRING,       -- public for PostgreSQL
# MAGIC     source_table    STRING,       -- source table name
# MAGIC     source_path     STRING,       -- blob path for file sources
# MAGIC
# MAGIC     target_layer    STRING,       -- silver / gold
# MAGIC
# MAGIC     bronze_schema   STRING,       -- bronze
# MAGIC     silver_schema   STRING,       -- silver
# MAGIC     gold_schema     STRING,       -- gold
# MAGIC
# MAGIC     active_flag     BOOLEAN,
# MAGIC     load_order      INT,
# MAGIC     created_at      TIMESTAMP
# MAGIC
# MAGIC )
# MAGIC
# MAGIC USING DELTA;
# MAGIC
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- 2️⃣ metadata.table_parameters
# MAGIC -- Processing configuration
# MAGIC -- =====================================================
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS banking.metadata.table_parameters (
# MAGIC
# MAGIC     table_id        INT,
# MAGIC     parameter_name  STRING,       -- load_type / primary_key / watermark_column
# MAGIC     parameter_value STRING,
# MAGIC     created_at      TIMESTAMP
# MAGIC
# MAGIC )
# MAGIC
# MAGIC USING DELTA;
# MAGIC
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- 3️⃣ metadata.table_watermarks
# MAGIC -- Stores last successful watermark per table
# MAGIC -- =====================================================
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS banking.metadata.table_watermarks (
# MAGIC
# MAGIC     table_id             INT,
# MAGIC     last_watermark_value STRING,
# MAGIC     last_updated_at      TIMESTAMP,
# MAGIC     last_run_id          BIGINT
# MAGIC
# MAGIC )
# MAGIC
# MAGIC USING DELTA
# MAGIC
# MAGIC PARTITIONED BY (table_id);
# MAGIC
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- 4️⃣ metadata.pipeline_runs
# MAGIC -- Execution audit table
# MAGIC -- =====================================================
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS banking.metadata.pipeline_runs (
# MAGIC
# MAGIC     run_id            BIGINT,
# MAGIC     table_id          INT,
# MAGIC     layer             STRING,       -- Bronze / Silver / Gold
# MAGIC     start_time        TIMESTAMP,
# MAGIC     end_time          TIMESTAMP,
# MAGIC     status            STRING,       -- SUCCESS / FAILED / INPROGRESS
# MAGIC     number_of_records BIGINT,
# MAGIC     error_message     STRING
# MAGIC
# MAGIC )
# MAGIC
# MAGIC USING DELTA
# MAGIC
# MAGIC PARTITIONED BY (table_id);
# MAGIC
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- DBTITLE 1,Create Source Volume
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- CREATE SOURCE SCHEMA
# MAGIC -- =====================================================
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS banking.source;
# MAGIC
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- CREATE VOLUME FOR SOURCE FILES
# MAGIC -- =====================================================
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS banking.source.volume;
# MAGIC
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- 2. INSERT INTO metadata.tables
# MAGIC -- =====================================================
# MAGIC
# MAGIC INSERT INTO banking.metadata.tables VALUES
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- 1. CUSTOMERS
# MAGIC -- Neon PostgreSQL
# MAGIC -- =====================================================
# MAGIC
# MAGIC (
# MAGIC     1,
# MAGIC     'customers',
# MAGIC     'postgresql',
# MAGIC     'public',
# MAGIC     'customers',
# MAGIC     NULL,
# MAGIC     'silver',
# MAGIC     'bronze',
# MAGIC     'silver',
# MAGIC     NULL,
# MAGIC     TRUE,
# MAGIC     1,
# MAGIC     current_timestamp()
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- 2. ACCOUNTS
# MAGIC -- Neon PostgreSQL
# MAGIC -- =====================================================
# MAGIC
# MAGIC (
# MAGIC     2,
# MAGIC     'accounts',
# MAGIC     'postgresql',
# MAGIC     'public',
# MAGIC     'accounts',
# MAGIC     NULL,
# MAGIC     'silver',
# MAGIC     'bronze',
# MAGIC     'silver',
# MAGIC     NULL,
# MAGIC     TRUE,
# MAGIC     2,
# MAGIC     current_timestamp()
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- 3. TRANSACTIONS
# MAGIC -- Neon PostgreSQL
# MAGIC -- =====================================================
# MAGIC
# MAGIC (
# MAGIC     3,
# MAGIC     'transactions',
# MAGIC     'postgresql',
# MAGIC     'public',
# MAGIC     'transactions',
# MAGIC     NULL,
# MAGIC     'silver',
# MAGIC     'bronze',
# MAGIC     'silver',
# MAGIC     NULL,
# MAGIC     TRUE,
# MAGIC     3,
# MAGIC     current_timestamp()
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- 4. BRANCHES
# MAGIC -- Neon PostgreSQL - FULL LOAD
# MAGIC -- =====================================================
# MAGIC
# MAGIC (
# MAGIC     4,
# MAGIC     'branches',
# MAGIC     'postgresql',
# MAGIC     'public',
# MAGIC     'branches',
# MAGIC     NULL,
# MAGIC     'silver',
# MAGIC     'bronze',
# MAGIC     'silver',
# MAGIC     NULL,
# MAGIC     TRUE,
# MAGIC     4,
# MAGIC     current_timestamp()
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- 5. CREDIT BUREAU REPORTS
# MAGIC -- Blob / CSV
# MAGIC -- =====================================================
# MAGIC
# MAGIC (
# MAGIC     5,
# MAGIC     'credit_bureau_reports',
# MAGIC     'blob',
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     '/Volumes/banking/source/volume/credit_bureau_reports/',
# MAGIC     'silver',
# MAGIC     'bronze',
# MAGIC     'silver',
# MAGIC     NULL,
# MAGIC     TRUE,
# MAGIC     5,
# MAGIC     current_timestamp()
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- 6. PAYMENT GATEWAY LOGS
# MAGIC -- Blob / CSV
# MAGIC -- =====================================================
# MAGIC
# MAGIC (
# MAGIC     6,
# MAGIC     'payment_gateway_logs',
# MAGIC     'blob',
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     '/Volumes/banking/source/volume/payment_gateway_logs/',
# MAGIC     'silver',
# MAGIC     'bronze',
# MAGIC     'silver',
# MAGIC     NULL,
# MAGIC     TRUE,
# MAGIC     6,
# MAGIC     current_timestamp()
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- DBTITLE 1,Insert Table Parameters
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- 3. INSERT INTO metadata.table_parameters
# MAGIC -- =====================================================
# MAGIC
# MAGIC INSERT INTO banking.metadata.table_parameters VALUES
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- CUSTOMERS
# MAGIC -- MERGE + WATERMARK
# MAGIC -- =====================================================
# MAGIC
# MAGIC (1, 'load_type', 'MERGE', current_timestamp()),
# MAGIC
# MAGIC (1, 'primary_key', 'customer_id', current_timestamp()),
# MAGIC
# MAGIC (1, 'watermark_column', 'updated_at', current_timestamp()),
# MAGIC
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- ACCOUNTS
# MAGIC -- MERGE + WATERMARK
# MAGIC -- =====================================================
# MAGIC
# MAGIC (2, 'load_type', 'MERGE', current_timestamp()),
# MAGIC
# MAGIC (2, 'primary_key', 'account_id', current_timestamp()),
# MAGIC
# MAGIC (2, 'watermark_column', 'updated_at', current_timestamp()),
# MAGIC
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- TRANSACTIONS
# MAGIC -- APPEND + WATERMARK
# MAGIC -- =====================================================
# MAGIC
# MAGIC (3, 'load_type', 'APPEND', current_timestamp()),
# MAGIC
# MAGIC (3, 'primary_key', 'txn_id', current_timestamp()),
# MAGIC
# MAGIC (3, 'watermark_column', 'txn_timestamp', current_timestamp()),
# MAGIC
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- BRANCHES
# MAGIC -- FULL LOAD
# MAGIC -- =====================================================
# MAGIC
# MAGIC (4, 'load_type', 'FULL', current_timestamp()),
# MAGIC
# MAGIC (4, 'primary_key', 'branch_code', current_timestamp()),
# MAGIC
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- CREDIT BUREAU REPORTS
# MAGIC -- MERGE + WATERMARK
# MAGIC -- =====================================================
# MAGIC
# MAGIC (5, 'load_type', 'MERGE', current_timestamp()),
# MAGIC
# MAGIC (5, 'primary_key', 'customer_id', current_timestamp()),
# MAGIC
# MAGIC (5, 'watermark_column', 'bureau_pull_date', current_timestamp()),
# MAGIC
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- PAYMENT GATEWAY LOGS
# MAGIC -- APPEND + WATERMARK
# MAGIC -- =====================================================
# MAGIC
# MAGIC (6, 'load_type', 'APPEND', current_timestamp()),
# MAGIC
# MAGIC (6, 'primary_key', 'txn_id', current_timestamp()),
# MAGIC
# MAGIC (6, 'watermark_column', 'processed_timestamp', current_timestamp());
# MAGIC
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- 4. INITIALIZE WATERMARK TABLE
# MAGIC -- Only for incremental tables
# MAGIC -- =====================================================
# MAGIC
# MAGIC INSERT INTO banking.metadata.table_watermarks VALUES
# MAGIC
# MAGIC (1, '1900-01-01 00:00:00', current_timestamp(), NULL),
# MAGIC
# MAGIC (2, '1900-01-01 00:00:00', current_timestamp(), NULL),
# MAGIC
# MAGIC (3, '1900-01-01 00:00:00', current_timestamp(), NULL),
# MAGIC
# MAGIC (5, '1900-01-01 00:00:00', current_timestamp(), NULL),
# MAGIC
# MAGIC (6, '1900-01-01 00:00:00', current_timestamp(), NULL);
# MAGIC
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- DBTITLE 1,Gold Metadata
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- 5. CUSTOMER 360
# MAGIC -- Silver → Gold
# MAGIC -- =====================================================
# MAGIC
# MAGIC INSERT INTO banking.metadata.tables VALUES
# MAGIC
# MAGIC (
# MAGIC     7,
# MAGIC     'customer_360',
# MAGIC     'silver',
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     'gold',
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     'gold',
# MAGIC     TRUE,
# MAGIC     1,
# MAGIC     current_timestamp()
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- 6. BRANCH PERFORMANCE
# MAGIC -- Silver → Gold
# MAGIC -- =====================================================
# MAGIC
# MAGIC INSERT INTO banking.metadata.tables VALUES
# MAGIC
# MAGIC (
# MAGIC     8,
# MAGIC     'branch_performance',
# MAGIC     'silver',
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     'gold',
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     'gold',
# MAGIC     TRUE,
# MAGIC     2,
# MAGIC     current_timestamp()
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- 7. TRANSACTION CHANNEL SUMMARY
# MAGIC -- Silver → Gold
# MAGIC -- =====================================================
# MAGIC
# MAGIC (
# MAGIC     9,
# MAGIC     'transaction_channel_summary',
# MAGIC     'silver',
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     'gold',
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     'gold',
# MAGIC     TRUE,
# MAGIC     3,
# MAGIC     current_timestamp()
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- 8. DAILY BANK KPI
# MAGIC -- Silver → Gold
# MAGIC -- =====================================================
# MAGIC
# MAGIC (
# MAGIC     10,
# MAGIC     'daily_bank_kpi',
# MAGIC     'silver',
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     'gold',
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     'gold',
# MAGIC     TRUE,
# MAGIC     4,
# MAGIC     current_timestamp()
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- 9. RISK CUSTOMER SUMMARY
# MAGIC -- Silver → Gold
# MAGIC -- =====================================================
# MAGIC
# MAGIC INSERT INTO banking.metadata.tables VALUES
# MAGIC
# MAGIC (
# MAGIC     11,
# MAGIC     'risk_customer_summary',
# MAGIC     'silver',
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     'gold',
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     'gold',
# MAGIC     TRUE,
# MAGIC     1,
# MAGIC     current_timestamp()
# MAGIC );
# MAGIC