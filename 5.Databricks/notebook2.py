# Databricks notebook source
# MAGIC %run "./notebook1" $file_path="/FileStore/tables/dept.csv" $table_name="dept"

# COMMAND ----------

# MAGIC %run "./notebook1" $file_path="dbfs:/FileStore/tables/cusotmer_source.csv" $table_name="cusotmer_source"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cusotmer_source
