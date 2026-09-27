# Databricks notebook source
spark.sql("CREATE SCHEMA IF NOT EXISTS banking.gold")

spark.sql("""
CREATE OR REPLACE TABLE banking.gold.branch_performance AS

WITH customer_branch AS (
    SELECT
        c.customer_id,
        c.branch_code
    FROM banking.silver.customers c
),

account_agg AS (
    SELECT
        a.customer_id,
        COUNT(a.account_id) AS total_accounts,
        SUM(a.balance) AS total_balance
    FROM banking.silver.accounts a
    GROUP BY a.customer_id
),

txn_agg AS (
    SELECT
        a.customer_id,
        COUNT(t.txn_id) AS total_transactions,
        SUM(t.amount) AS total_transaction_amount
    FROM banking.silver.transactions t
    JOIN banking.silver.accounts a
        ON t.account_id = a.account_id
    GROUP BY a.customer_id
)

SELECT
    b.branch_code,
    b.branch_name,
    COUNT(DISTINCT cb.customer_id) AS total_customers,
    SUM(a.total_accounts) AS total_accounts,
    SUM(a.total_balance) AS total_deposits,
    SUM(t.total_transactions) AS total_transactions,
    SUM(t.total_transaction_amount) AS total_transaction_amount

FROM banking.silver.branches b

LEFT JOIN customer_branch cb
    ON b.branch_code = cb.branch_code

LEFT JOIN account_agg a
    ON cb.customer_id = a.customer_id

LEFT JOIN txn_agg t
    ON cb.customer_id = t.customer_id

GROUP BY
    b.branch_code,
    b.branch_name
""")

count = spark.sql("""
SELECT COUNT(*) AS cnt
FROM banking.gold.branch_performance
""").collect()[0]["cnt"]

dbutils.notebook.exit(str(count))