# Databricks notebook source
# MAGIC %run ./reusable_notebook $path="dbfs:/FileStore/tables/corona_tweets.csv" $table="corona_tweets"

# COMMAND ----------

dbutils.notebook.run("./reusable_notebook",timeout_seconds=875034)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from apple_store;

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/

# COMMAND ----------

# MAGIC %fs head dbfs:/FileStore/tables/corona_tweets.csv

# COMMAND ----------

display(spark.read.text("dbfs:/FileStore/tables/corona_tweets.csv"))
