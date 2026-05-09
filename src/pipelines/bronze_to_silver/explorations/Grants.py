# Databricks notebook source
# MAGIC %sql
# MAGIC ALTER STREAMING TABLE uber_catalog.silver.stg_rides
# MAGIC SET TBLPROPERTIES (
# MAGIC     'pipelines.pipelineId' = 'f2afba3b-3775-4620-8451-ab5fa6ed2ecd'
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED uber_catalog.silver.stg_rides;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER STREAMING TABLE uber_catalog.silver.silver_obt
# MAGIC SET TBLPROPERTIES (
# MAGIC     'pipelines.pipelineId' = 'f2afba3b-3775-4620-8451-ab5fa6ed2ecd'
# MAGIC );