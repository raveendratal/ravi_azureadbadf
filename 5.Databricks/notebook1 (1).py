# Databricks notebook source
dbutils.widgets.text("file_path","/FileStore/tables/emp.csv")
dbutils.widgets.text("table_name","emp_csv")

# COMMAND ----------

file_path=dbutils.widgets.get("file_path")
table_name=dbutils.widgets.get("table_name")

# COMMAND ----------

df = spark.read.csv(file_path,header=True,inferSchema=True)

# COMMAND ----------

display(df)

# COMMAND ----------

print('Data Frame Created : ',df)

# COMMAND ----------

df.write.format("csv").mode("append").saveAsTable(table_name)

# COMMAND ----------

print("data appended into table : ",table_name)
print("No of rows appended into target table : ",df.count())

# COMMAND ----------

No_rows = spark.sql("select count(*) from {0}".format(table_name)).collect()[0][0]
print('No of Rows available in Target table : ',No_rows)

# COMMAND ----------

dbutils.notebook.exit(No_rows)
