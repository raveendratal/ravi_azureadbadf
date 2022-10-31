# Databricks notebook source
# MAGIC %sql
# MAGIC select * from apple_store

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to call one notebook in another notebook for reusability

# COMMAND ----------

# MAGIC %run ./reusable_notebook $path="dbfs:/FileStore/tables/AppleStore.csv" $table="apple_store"

# COMMAND ----------

dbutils.notebook.run("./reusable_notebook",400,{"path":"/FileStore/tables/emp.csv","table":"employee"})

# COMMAND ----------

help(dbutils.notebook)
