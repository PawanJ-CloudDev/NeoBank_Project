# Databricks notebook source
# MAGIC %sql
# MAGIC select * from banking.metadata.tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from banking.metadata.table_parameters
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from banking.metadata.pipeline_runs order by start_time desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from banking.metadata.table_parameters where table_id = 4

# COMMAND ----------

# MAGIC %sql
# MAGIC select t.table_name, table_parameters.parameter_value from banking.metadata.tables t join banking.metadata.table_parameters where t.table_id = table_parameters.table_id and table_parameters.parameter_name='load_type'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from banking.metadata.table_watermarks