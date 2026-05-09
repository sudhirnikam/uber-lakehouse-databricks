# Databricks notebook source
spark.sql("select * from uber_catalog.bronze.map_cancellation_reasons").show(10, False)

# COMMAND ----------

spark.sql("select count(*) from uber_catalog.silver.stg_rides").show(10, False)

# COMMAND ----------

spark.sql("select count(*) from uber_catalog.bronze.rides_raw").show(10, False)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber_catalog.bronze.map_cities;