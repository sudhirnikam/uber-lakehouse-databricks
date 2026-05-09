# Databricks notebook source
# MAGIC %sql
# MAGIC select count(*) from uber_catalog.gold.fact;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber_catalog.gold.fact limit 5;