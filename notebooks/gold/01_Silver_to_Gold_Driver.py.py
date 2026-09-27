# Databricks notebook source
import json
from datetime import datetime, timezone

run_id = dbutils.widgets.get("run_id")
table_metadata = dbutils.widgets.get("table_metadata")

if not run_id or run_id == "auto":
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

table_metadata = json.loads(table_metadata.replace("'", '"'))

table_id = table_metadata["table_id"]
table_name = table_metadata["table_name"]

start_time = datetime.now(timezone.utc)

print("Run ID:", run_id)
print("Table ID:", table_id)
print("Table Name:", table_name)


# COMMAND ----------

run_id = int(str(run_id).replace("_", ""))
table_id = int(table_id)

entry_exists = spark.sql(f"""
    SELECT 1
    FROM banking.metadata.pipeline_runs
    WHERE run_id = {run_id}
    AND table_id = {table_id}
""").count() > 0

if entry_exists:
    
    spark.sql(f"""
        UPDATE banking.metadata.pipeline_runs
        SET
            layer = 'Gold',
            start_time = TIMESTAMP('{start_time}'),
            end_time = NULL,
            status = 'INPROGRESS',
            number_of_records = NULL,
            error_message = NULL
        WHERE run_id = {run_id}
        AND table_id = {table_id}
    """)

else:

    spark.sql(f"""
        INSERT INTO banking.metadata.pipeline_runs
        VALUES (
            {run_id},
            {table_id},
            'Gold',
            TIMESTAMP('{start_time}'),
            NULL,
            'INPROGRESS',
            NULL,
            NULL
        )
    """)

print("Audit entry created / updated")

# COMMAND ----------

print("Gold table to build:", table_name)

# COMMAND ----------

status = "SUCCESS"
error_message = None
records = None

# SQL definitions for each gold table (inlined because dbutils.notebook.run

gold_sql = {
    "daily_bank_kpi": """
        CREATE OR REPLACE TABLE banking.gold.daily_bank_kpi AS
        WITH txn_daily AS (
            SELECT
                DATE(txn_timestamp) AS txn_date,
                COUNT(txn_id) AS total_transactions,
                SUM(amount) AS total_transaction_amount
            FROM banking.silver.transactions
            GROUP BY DATE(txn_timestamp)
        ),
        customer_metrics AS (
            SELECT COUNT(DISTINCT customer_id) AS total_customers
            FROM banking.silver.customers
        ),
        account_metrics AS (
            SELECT
                COUNT(account_id) AS total_accounts,
                SUM(balance) AS total_balance
            FROM banking.silver.accounts
        ),
        credit_metrics AS (
            SELECT
                AVG(credit_score) AS avg_credit_score,
                SUM(CASE WHEN risk_grade = 'HIGH' THEN 1 ELSE 0 END) AS high_risk_customers
            FROM banking.silver.credit_bureau_reports
        )
        SELECT
            t.txn_date,
            cm.total_customers,
            am.total_accounts,
            am.total_balance,
            t.total_transactions,
            t.total_transaction_amount,
            cr.avg_credit_score,
            cr.high_risk_customers
        FROM txn_daily t
        CROSS JOIN customer_metrics cm
        CROSS JOIN account_metrics am
        CROSS JOIN credit_metrics cr
    """,
    "risk_customer_summary": """
        CREATE OR REPLACE TABLE banking.gold.risk_customer_summary AS
        SELECT
            risk_grade,
            COUNT(customer_id) AS total_customers,
            AVG(credit_score) AS avg_credit_score,
            SUM(external_active_loans) AS total_external_loans,
            SUM(external_overdue_amount) AS total_overdue_amount
        FROM banking.silver.credit_bureau_reports
        GROUP BY risk_grade
    """,
    "transaction_channel_summary": """
        CREATE OR REPLACE TABLE banking.gold.transaction_channel_summary AS
        SELECT
            DATE(t.txn_timestamp) AS txn_date,
            pg.gateway_name,
            pg.device_type,
            COUNT(*) AS total_transactions,
            SUM(CASE WHEN pg.gateway_status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_transactions,
            SUM(CASE WHEN pg.gateway_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_transactions,
            AVG(pg.processing_time_ms) AS avg_processing_time_ms
        FROM banking.silver.transactions t
        JOIN banking.silver.payment_gateway_logs pg
            ON t.txn_id = pg.txn_id
        GROUP BY txn_date, pg.gateway_name, pg.device_type
    """,
    "customer_360": """
        CREATE OR REPLACE TABLE banking.gold.customer_360 AS
        WITH customer_accounts AS (
            SELECT
                customer_id,
                COUNT(account_id) AS total_accounts,
                SUM(balance) AS total_balance
            FROM banking.silver.accounts
            GROUP BY customer_id
        ),
        customer_transactions AS (
            SELECT
                a.customer_id,
                COUNT(t.txn_id) AS total_transactions,
                SUM(t.amount) AS total_transaction_amount
            FROM banking.silver.transactions t
            JOIN banking.silver.accounts a ON t.account_id = a.account_id
            GROUP BY a.customer_id
        ),
        customer_branch AS (
            SELECT
                c.customer_id,
                CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
                b.branch_name
            FROM banking.silver.customers c
            LEFT JOIN banking.silver.branches b ON c.branch_code = b.branch_code
        )
        SELECT
            cb.customer_id,
            cb.customer_name,
            cb.branch_name,
            COALESCE(ca.total_accounts, 0) AS total_accounts,
            COALESCE(ca.total_balance, 0) AS total_balance,
            COALESCE(ct.total_transactions, 0) AS total_transactions,
            COALESCE(ct.total_transaction_amount, 0) AS total_transaction_amount,
            cr.credit_score,
            cr.risk_grade,
            cr.external_active_loans,
            cr.external_overdue_amount,
            CASE
                WHEN cr.risk_grade = 'HIGH' THEN 'High Risk'
                WHEN cr.risk_grade = 'MEDIUM' THEN 'Medium Risk'
                ELSE 'Low Risk'
            END AS customer_segment
        FROM customer_branch cb
        LEFT JOIN customer_accounts ca ON cb.customer_id = ca.customer_id
        LEFT JOIN customer_transactions ct ON cb.customer_id = ct.customer_id
        LEFT JOIN banking.silver.credit_bureau_reports cr ON cb.customer_id = cr.customer_id
    """,
    "branch_performance": """
        CREATE OR REPLACE TABLE banking.gold.branch_performance AS
        WITH branch_customers AS (
            SELECT branch_code, COUNT(*) AS total_customers
            FROM banking.silver.customers
            GROUP BY branch_code
        ),
        branch_accounts AS (
            SELECT branch_code, COUNT(*) AS total_accounts, SUM(balance) AS total_deposits
            FROM banking.silver.accounts
            GROUP BY branch_code
        ),
        branch_transactions AS (
            SELECT
                a.branch_code,
                COUNT(t.txn_id) AS total_transactions,
                SUM(t.amount) AS total_transaction_amount
            FROM banking.silver.transactions t
            JOIN banking.silver.accounts a ON t.account_id = a.account_id
            GROUP BY a.branch_code
        )
        SELECT
            b.branch_code,
            b.branch_name,
            COALESCE(bc.total_customers, 0) AS total_customers,
            COALESCE(ba.total_accounts, 0) AS total_accounts,
            COALESCE(ba.total_deposits, 0) AS total_deposits,
            COALESCE(bt.total_transactions, 0) AS total_transactions,
            COALESCE(bt.total_transaction_amount, 0) AS total_transaction_amount
        FROM banking.silver.branches b
        LEFT JOIN branch_customers bc ON b.branch_code = bc.branch_code
        LEFT JOIN branch_accounts ba ON b.branch_code = ba.branch_code
        LEFT JOIN branch_transactions bt ON b.branch_code = bt.branch_code
    """,
}

try:

    if table_name not in gold_sql:
        raise ValueError(f"No SQL defined for table: {table_name}")

   
    spark.sql(gold_sql[table_name])

    # Get record count
    records = spark.sql(f"SELECT COUNT(*) AS cnt FROM banking.gold.{table_name}").collect()[0]["cnt"]

    print("Gold transformation completed successfully")
    print("Records:", records)

except Exception as e:

    status = "FAILED"
    error_message = str(e)

    print("Gold transformation failed")
    print(error_message)

# COMMAND ----------

# DBTITLE 1,Update audit status
# ==========================================
# Update pipeline_runs audit entry with final status
# ==========================================

end_time = datetime.now(timezone.utc)

records_sql = f"NULL" if records is None else str(records)
error_sql = "NULL" if error_message is None else f"'{error_message.replace("'", "''")}'"

spark.sql(f"""
    UPDATE banking.metadata.pipeline_runs
    SET
        end_time = TIMESTAMP('{end_time}'),
        status = '{status}',
        number_of_records = {records_sql},
        error_message = {error_sql}
    WHERE run_id = {run_id}
    AND table_id = {table_id}
""")

print(f"Audit updated: status={status}, records={records}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM banking.gold.daily_bank_kpi;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) 
# MAGIC FROM banking.gold.daily_bank_kpi;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT DATE(txn_timestamp)) AS distinct_dates
# MAGIC FROM banking.silver.transactions;