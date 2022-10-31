# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("path","/FileStore/tables/emp.csv")
v_path=dbutils.widgets.get("path")
print('Path PArameter Value : ',v_path)
dbutils.widgets.text("table","emp")
v_table_name=dbutils.widgets.get("table")
print('Table Parameter Value : ',v_table_name)

# COMMAND ----------

df = spark.read.csv(v_path,header=True,inferSchema=True,sep=",").dropDuplicates()
print('Data Frame is created : ',df.printSchema())

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable(v_table_name)
v_count = df.count()
print('No of records  {1} are appended in this {0} table '.format(v_table_name,v_count))

# COMMAND ----------

dbutils.notebook.exit("stopped")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp
