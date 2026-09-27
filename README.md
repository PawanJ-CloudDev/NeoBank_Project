# NeoBank — Banking Data Engineering Project

A metadata-driven, scalable banking data pipeline on Databricks that ingests data from multiple sources, processes it through Bronze -> Silver -> Gold medallion layers, monitors every job execution, and delivers business insights via SQL Dashboards and Genie.

## Architecture Overview

```
Data Sources -> Ingestion -> Bronze -> Silver -> Gold -> Consumption
                                  ^
                          Metadata Framework
                          Job Orchestration
                          Monitoring
```

## Project Structure

```
NeoBank_Project/
├── notebooks/
│   ├── metadata/              Metadata framework notebooks
│   │   ├── 01_setup_metadata       Create metadata tables and load config
│   │   └── 02_check_metadata       Validate metadata tables
│   ├── bronze/                Bronze ingestion notebooks
│   │   ├── 01_Read_Tables_List     Read active tables from metadata (widget-driven)
│   │   ├── 02_read_table_parameters Read load_type, primary_key, watermark_column
│   │   └── 03_source_to_bronze     Ingest source data into Bronze Delta tables
│   ├── silver/                Silver transformation notebooks
│   │   └── 04_bronze_to_silver     Cleanse, dedupe, enrich, SCD handling
│   ├── gold/                  Gold transformation notebooks
│   │   ├── 01_Silver_to_Gold_Driver Driver notebook for Gold layer
│   │   ├── customer_360            360-degree customer view
│   │   ├── branch_performance       Branch-level performance metrics
│   │   ├── transaction_channel_summary  Transactions by channel
│   │   ├── daily_bank_kpi          Daily bank-wide KPIs
│   │   └── risk_customer_summary    Customer risk profiles
│   ├── orchestration/         Job orchestration notebooks (placeholder)
│   └── utils/                 Shared utility notebooks
│       ├── 00_Setup_Secret_Scope   Databricks secret scope setup
│       └── 01_Send_Email           Email notification on success/failure
├── config/                    Connection configs, job parameters
├── sql/                       DDL, DQ rules, dashboard queries
├── docs/                      Documentation
│   └── NeoBank_Architecture_Diagram  Architecture diagrams (Mermaid)
└── tests/                     Pipeline test notebooks
```

## Data Sources

| Source | Tables | Ingestion Method |
| --- | --- | --- |
| Neon PostgreSQL | customers, accounts, transactions, branches | JDBC / Batch |
| Blob / File Storage | credit_bureau_reports, payment_gateway_logs | Auto Loader |

## Unity Catalog — banking

| Schema | Purpose |
| --- | --- |
| banking.metadata | Metadata-driven framework: tables, table_parameters, table_watermarks, pipeline_runs |
| banking.source | External source connection references and volumes |
| banking.bronze | Raw / near-raw Delta tables (source-aligned) |
| banking.silver | Cleansed, standardized, deduplicated, business-transformed |
| banking.gold | Curated business tables for analytics and reporting |

## Metadata Framework

The pipeline is fully metadata-driven — no hardcoded table names or load logic.

| Table | Role |
| --- | --- |
| metadata.tables | Source-to-target mapping, load order, active flag |
| metadata.table_parameters | load_type (full/incremental), primary_key, watermark_column |
| metadata.table_watermarks | Last successful watermark for incremental processing |
| metadata.pipeline_runs | Full audit: run_id, table_id, layer, start/end_time, status, record_count, error_message |

## Pipeline Flow

1. **Read Metadata** — Query metadata.tables + table_parameters for active tables
2. **Bronze Ingestion** — JDBC batch or Auto Loader into Bronze Delta tables
3. **Update Audit** — Log run details to pipeline_runs
4. **Silver Transformation** — Cleanse, deduplicate, enrich, SCD Type 2
5. **Update Watermark** — Store new max watermark in table_watermarks
6. **Gold Transformation** — Build curated business tables
7. **DQ Validation** — Data quality checks on Gold tables
8. **Email Notification** — Success or failure alert

## Gold Tables

| Table | Description |
| --- | --- |
| gold.customer_360 | Customer profile + accounts + transactions + credit info |
| gold.branch_performance | Branch volume, value, customer count |
| gold.transaction_channel_summary | Aggregates by channel (online, ATM, branch, mobile) |
| gold.daily_bank_kpi | Daily deposits, withdrawals, active accounts, new customers |
| gold.risk_customer_summary | Credit scores, anomalies, high-risk flags |

## Consumption

* **Databricks SQL Dashboards** — Pre-built KPI dashboards for analysts and executives
* **Genie Workspace** — Natural language Q&A for business users (e.g., "What is the total transaction amount?")

## Setup

1. Run `notebooks/utils/00_Setup_Secret_Scope.py` to configure the Databricks secret scope
2. Run `notebooks/metadata/01_setup_metadata` to create metadata tables and load configuration
3. Run `notebooks/metadata/02_check_metadata` to validate metadata
4. Configure the NeoBank_Master_Job Databricks Job with notebook tasks in order
5. Run the job or trigger via schedule

## Connection Details

* **Neon PostgreSQL**: ep-flat-flower-b52fwnhv-pooler.c-7.us-east-2.aws.neon.tech:5432
* **Database**: neondb
* **Secret Scope**: banking-scope (key: postgres-connection-json)
* **Cloud Provider**: AWS

## License

Internal project — NeoBank Data Engineering Team