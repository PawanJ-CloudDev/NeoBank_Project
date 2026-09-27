# Databricks notebook source
# DBTITLE 1,Create customer_360 gold table
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE banking.gold.customer_360 AS
# MAGIC
# MAGIC WITH account_agg AS (
# MAGIC     SELECT
# MAGIC         customer_id,
# MAGIC         COUNT(account_id) AS total_accounts,
# MAGIC         SUM(balance) AS total_balance
# MAGIC     FROM banking.silver.accounts
# MAGIC     GROUP BY customer_id
# MAGIC ),
# MAGIC
# MAGIC txn_agg AS (
# MAGIC     SELECT
# MAGIC         a.customer_id,
# MAGIC         COUNT(t.txn_id) AS total_transactions,
# MAGIC         SUM(t.amount) AS total_transaction_amount
# MAGIC     FROM banking.silver.transactions t
# MAGIC     JOIN banking.silver.accounts a
# MAGIC         ON t.account_id = a.account_id
# MAGIC     GROUP BY a.customer_id
# MAGIC ),
# MAGIC
# MAGIC credit_latest AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY customer_id
# MAGIC                 ORDER BY bureau_pull_date DESC
# MAGIC             ) AS rn
# MAGIC         FROM banking.silver.credit_bureau_reports
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     c.customer_id,
# MAGIC     CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
# MAGIC     b.branch_name,
# MAGIC     COALESCE(a.total_accounts, 0) AS total_accounts,
# MAGIC     COALESCE(a.total_balance, 0) AS total_balance,
# MAGIC     COALESCE(t.total_transactions, 0) AS total_transactions,
# MAGIC     COALESCE(t.total_transaction_amount, 0) AS total_transaction_amount,
# MAGIC     COALESCE(cr.credit_score, 'N/A') AS credit_score,
# MAGIC     COALESCE(cr.risk_grade, 'UNKNOWN') AS risk_grade,
# MAGIC     COALESCE(cr.external_active_loans, 'N/A') AS external_active_loans,
# MAGIC     COALESCE(cr.external_overdue_amount, 'N/A') AS external_overdue_amount,
# MAGIC     CASE
# MAGIC         WHEN a.total_balance >= 500000 THEN 'HIGH_VALUE'
# MAGIC         WHEN a.total_balance >= 100000 THEN 'MEDIUM_VALUE'
# MAGIC         ELSE 'LOW_VALUE'
# MAGIC     END AS customer_segment
# MAGIC FROM banking.silver.customers c
# MAGIC LEFT JOIN account_agg a
# MAGIC     ON c.customer_id = a.customer_id
# MAGIC LEFT JOIN txn_agg t
# MAGIC     ON c.customer_id = t.customer_id
# MAGIC LEFT JOIN banking.silver.branches b
# MAGIC     ON c.branch_code = b.branch_code
# MAGIC LEFT JOIN credit_latest cr
# MAGIC     ON c.customer_id = cr.customer_id;

# COMMAND ----------

count = spark.sql("""
SELECT COUNT(*) AS cnt
FROM banking.gold.customer_360
""").collect()[0]["cnt"]

dbutils.notebook.exit(str(count))

# COMMAND ----------

# DBTITLE 1,Find NULL risk_grade customers
# MAGIC %sql --name null_risk_customers
# MAGIC SELECT customer_id, customer_name, total_balance, total_accounts, credit_score, risk_grade
# MAGIC FROM banking.gold.customer_360
# MAGIC WHERE risk_grade IS NULL

# COMMAND ----------

# DBTITLE 1,Check credit_bureau_reports for these customers
# MAGIC %sql --name credit_check
# MAGIC SELECT customer_id, credit_score, risk_grade, bureau_pull_date
# MAGIC FROM banking.silver.credit_bureau_reports
# MAGIC WHERE customer_id IN (1, 2)

# COMMAND ----------

# DBTITLE 1,Verify no NULL risk_grade values
# MAGIC %sql --name verify_risk_grades
# MAGIC SELECT risk_grade, COUNT(*) AS customers
# MAGIC FROM banking.gold.customer_360
# MAGIC GROUP BY risk_grade
# MAGIC ORDER BY customers DESC