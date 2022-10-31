# Databricks notebook source
dbutils.widgets.text("path","/FileStore/tables/emp.csv")
dbutils.widgets.text("header","true")
dbutils.widgets.text("sep",",")
dbutils.widgets.text("inferSchema","true")
dbutils.widgets.text("table_name","emp")
v_path = dbutils.widgets.get("path")
v_header = dbutils.widgets.get("header")
v_sep = dbutils.widgets.get("sep")
v_inferSchema = dbutils.widgets.get("inferSchema")
v_table_name = dbutils.widgets.get("table_name")

# COMMAND ----------

print('v')

# COMMAND ----------

df = spark.read.csv(v_path,sep=v_sep,header=v_header,inferSchema=v_inferSchema)

# COMMAND ----------

from pyspark.sql.functions import lit,current_date
df = df.distinct()
df = df.withColumn("CREATEDBY",lit("ETL_JOB")).withColumn("CREATED_DATE",current_date())

# COMMAND ----------

print("before Dbutils.notebook.exit")

# COMMAND ----------

dbutils.notebook.exit("any message we can give")

# COMMAND ----------

print("after Dbutils.notebook.exit")

# COMMAND ----------

print("after Dbutils.notebook.exit")

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable(v_table_name)
