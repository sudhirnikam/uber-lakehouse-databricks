# Databricks notebook source
files = dbutils.fs.ls("abfss://raw@dluberlakehousedev.dfs.core.windows.net/ingestion/")
display(files)

# COMMAND ----------

files = dbutils.fs.ls("abfss://raw@dluberlakehousedev.dfs.core.windows.net/ingestion/")
for f in files:
    print(f.name)