# Databricks notebook source
# DBTITLE 1,NeoBank Architecture
# MAGIC %md
# MAGIC # NeoBank - Banking Data Engineering Architecture
# MAGIC
# MAGIC Metadata-driven pipeline: multi-source ingestion -> Bronze/Silver/Gold medallion -> metadata audit -> SQL Dashboards and Genie

# COMMAND ----------

# DBTITLE 1,Master Architecture Diagram
# MAGIC %md
# MAGIC ## End-to-End Architecture
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart TB
# MAGIC     subgraph DS["1. Data Sources"]
# MAGIC         PG["Neon PostgreSQL<br/>customers, accounts,<br/>transactions, branches"]
# MAGIC         BLOB["Blob Storage<br/>credit_bureau_reports,<br/>payment_gateway_logs"]
# MAGIC     end
# MAGIC
# MAGIC     JDBC["2a. JDBC / Batch"]
# MAGIC     AL["2b. Auto Loader"]
# MAGIC
# MAGIC     META["4. Metadata Framework<br/>banking.metadata<br/>tables, table_parameters,<br/>table_watermarks, pipeline_runs"]
# MAGIC
# MAGIC     subgraph UC["3. Unity Catalog: banking"]
# MAGIC         direction LR
# MAGIC         S1["metadata"]
# MAGIC         S2["source"]
# MAGIC         S3["bronze"]
# MAGIC         S4["silver"]
# MAGIC         S5["gold"]
# MAGIC     end
# MAGIC
# MAGIC     subgraph BRZ["5. Bronze"]
# MAGIC         B["6 raw Delta tables<br/>source-aligned"]
# MAGIC     end
# MAGIC     subgraph SLV["6. Silver"]
# MAGIC         SV["cleansed, deduped, SCD<br/>incremental"]
# MAGIC     end
# MAGIC     subgraph GLD["7. Gold"]
# MAGIC         GD["5 business tables"]
# MAGIC     end
# MAGIC
# MAGIC     JOB["8. Databricks Job<br/>NeoBank_Master_Job"]
# MAGIC     MON["9. Monitoring<br/>pipeline_runs + watermarks"]
# MAGIC
# MAGIC     subgraph CON["10. Consumption"]
# MAGIC         DASH["SQL Dashboards"]
# MAGIC         GENIE["Genie Workspace"]
# MAGIC     end
# MAGIC
# MAGIC     PG --> JDBC
# MAGIC     BLOB --> AL
# MAGIC     JDBC --> B
# MAGIC     AL --> B
# MAGIC     META -.-> JDBC
# MAGIC     META -.-> AL
# MAGIC     B --> SV
# MAGIC     SV --> GD
# MAGIC     JOB --> B
# MAGIC     JOB --> SV
# MAGIC     JOB --> GD
# MAGIC     JOB --> MON
# MAGIC     GD --> DASH
# MAGIC     GD --> GENIE
# MAGIC
# MAGIC     style DS fill:#e3f2fd,stroke:#1565c0,stroke-width:2px
# MAGIC     style META fill:#fff3e0,stroke:#e65100,stroke-width:2px
# MAGIC     style BRZ fill:#ffe0b2,stroke:#e65100,stroke-width:2px
# MAGIC     style SLV fill:#cfd8dc,stroke:#37474f,stroke-width:2px
# MAGIC     style GLD fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px
# MAGIC     style CON fill:#e1f5fe,stroke:#0277bd,stroke-width:2px
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Metadata Framework and Ingestion
# MAGIC %md
# MAGIC ## Metadata Framework and Ingestion
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart LR
# MAGIC     subgraph SRC["Sources"]
# MAGIC         PG["Neon PostgreSQL<br/>customers, accounts,<br/>transactions, branches"]
# MAGIC         BLOB["Blob Storage<br/>credit_bureau_reports,<br/>payment_gateway_logs"]
# MAGIC     end
# MAGIC
# MAGIC     subgraph MF["banking.metadata - drives everything"]
# MAGIC         T["tables<br/>source-to-target, load_order, is_active"]
# MAGIC         TP["table_parameters<br/>load_type, primary_key, watermark_column"]
# MAGIC         TW["table_watermarks<br/>last_watermark_value"]
# MAGIC         PR["pipeline_runs<br/>run_id, table_id, layer,<br/>status, records, error"]
# MAGIC     end
# MAGIC
# MAGIC     BRONZE["Bronze Layer"]
# MAGIC
# MAGIC     PG -->|JDBC| BRONZE
# MAGIC     BLOB -->|Auto Loader| BRONZE
# MAGIC     T -.-> Tp2["selects tables and order"]
# MAGIC     TP -.-> Tp3["load_type / PK / watermark"]
# MAGIC     TW -.-> Tp4["incremental cutoff"]
# MAGIC     Tp2 -.-> BRONZE
# MAGIC     Tp3 -.-> BRONZE
# MAGIC     Tp4 -.-> BRONZE
# MAGIC
# MAGIC     style MF fill:#fff3e0,stroke:#e65100,stroke-width:2px
# MAGIC ```
# MAGIC
# MAGIC | Metadata Table | Role |
# MAGIC | --- | --- |
# MAGIC | `tables` | Source-to-target mapping, load order, active flag |
# MAGIC | `table_parameters` | `load_type` (full/incremental), `primary_key`, `watermark_column` |
# MAGIC | `table_watermarks` | Last successful watermark for incremental processing |
# MAGIC | `pipeline_runs` | Full audit: run_id, table_id, layer, start/end_time, status, record_count, error_message |

# COMMAND ----------

# DBTITLE 1,Medallion Layers
# MAGIC %md
# MAGIC ## Medallion Layers - Bronze to Silver to Gold
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart LR
# MAGIC     subgraph BRZ["Bronze - banking.bronze"]
# MAGIC         B["Raw Delta tables<br/>customers, accounts, transactions,<br/>branches, credit_bureau_reports,<br/>payment_gateway_logs"]
# MAGIC     end
# MAGIC     subgraph SLV["Silver - banking.silver"]
# MAGIC         SV["Cleansed, Deduped, Standardized<br/>DQ checks, Incremental, SCD Type 2"]
# MAGIC     end
# MAGIC     subgraph GLD["Gold - banking.gold"]
# MAGIC         direction TB
# MAGIC         G1["customer_360"]
# MAGIC         G2["branch_performance"]
# MAGIC         G3["transaction_channel_summary"]
# MAGIC         G4["daily_bank_kpi"]
# MAGIC         G5["risk_customer_summary"]
# MAGIC     end
# MAGIC     B --> SV
# MAGIC     SV --> G1
# MAGIC     SV --> G2
# MAGIC     SV --> G3
# MAGIC     SV --> G4
# MAGIC     SV --> G5
# MAGIC
# MAGIC     style BRZ fill:#ffe0b2,stroke:#e65100,stroke-width:2px
# MAGIC     style SLV fill:#cfd8dc,stroke:#37474f,stroke-width:2px
# MAGIC     style GLD fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px
# MAGIC ```
# MAGIC
# MAGIC | Gold Table | Description |
# MAGIC | --- | --- |
# MAGIC | `customer_360` | Customer profile + accounts + transactions + credit info |
# MAGIC | `branch_performance` | Branch volume, value, customer count |
# MAGIC | `transaction_channel_summary` | Aggregates by channel (online, ATM, branch, mobile) |
# MAGIC | `daily_bank_kpi` | Daily deposits, withdrawals, active accounts, new customers |
# MAGIC | `risk_customer_summary` | Credit scores, anomalies, high-risk flags |

# COMMAND ----------

# DBTITLE 1,Job Orchestration, Monitoring and Consumption
# MAGIC %md
# MAGIC ## Job Orchestration, Monitoring and Consumption
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart TB
# MAGIC     START([Start]) --> RM["Read Metadata"]
# MAGIC     RM --> ID["Identify Active Tables<br/>ORDER BY load_order"]
# MAGIC     ID --> RS["Read Source"]
# MAGIC     RS --> BI["Bronze Ingestion"]
# MAGIC     BI --> A1["Update pipeline_runs"]
# MAGIC     A1 --> ST["Silver Transformation"]
# MAGIC     ST --> UW["Update watermarks"]
# MAGIC     UW --> GT["Gold Transformation"]
# MAGIC     GT --> A2["Update pipeline_runs"]
# MAGIC     A2 --> DQ["DQ Validation"]
# MAGIC     DQ --> CHK{"Success?"}
# MAGIC     CHK -->|Yes| OK["Status = COMPLETED"]
# MAGIC     CHK -->|No| FAIL["Status = FAILED<br/>+ error_message"]
# MAGIC     OK --> EMAIL["Email Notification"]
# MAGIC     FAIL --> EMAIL
# MAGIC     EMAIL --> END([End])
# MAGIC
# MAGIC     style OK fill:#c8e6c9,stroke:#2e7d32
# MAGIC     style FAIL fill:#ffcdd2,stroke:#c62828
# MAGIC     style CHK fill:#fff9c4,stroke:#f57f17,stroke-width:2px
# MAGIC     style A1 fill:#e3f2fd,stroke:#1565c0
# MAGIC     style A2 fill:#e3f2fd,stroke:#1565c0
# MAGIC     style UW fill:#e3f2fd,stroke:#1565c0
# MAGIC ```
# MAGIC
# MAGIC **Job:** `NeoBank_Master_Job` - notebook tasks with widgets/parameters (`run_id`, `layer`, `table_metadata`). Metadata-driven: reads `tables` + `table_parameters` to decide what, how, and in what order to process.
# MAGIC
# MAGIC **Monitoring:** `pipeline_runs` logs every run (status, record count, errors). `table_watermarks` tracks incremental progress. Databricks Jobs UI + email alerts.
# MAGIC
# MAGIC **Consumption:**
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart LR
# MAGIC     GD["Gold Layer"] --> DASH["SQL Dashboards<br/>KPIs and Reports"]
# MAGIC     GD --> GENIE["Genie Workspace<br/>Natural Language Q&A"]
# MAGIC     style GD fill:#c8e6c9,stroke:#2e7d32,stroke-width:2px
# MAGIC     style DASH fill:#e1f5fe,stroke:#0277bd,stroke-width:2px
# MAGIC     style GENIE fill:#f3e5f5,stroke:#6a1b9a,stroke-width:2px
# MAGIC ```
# MAGIC
# MAGIC **Genie examples:** *What is the total transaction amount?* / *Show daily transaction trend* / *Which branch has the highest volume this month?*

# COMMAND ----------

