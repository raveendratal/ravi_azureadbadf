-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text("file_path","/FileStore/tables/emp.csv")
-- MAGIC dbutils.widgets.text("table_name","emp_csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC file_path=dbutils.widgets.get("file_path")
-- MAGIC table_name=dbutils.widgets.get("table_name")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.csv(file_path,header=True,inferSchema=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print('Data Frame Created : ',df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format("csv").mode("append").saveAsTable(table_name)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print("data appended into table : ",table_name)
-- MAGIC print("No of rows appended into target table : ",df.count())

-- COMMAND ----------

-- MAGIC %python
-- MAGIC No_rows = spark.sql("select count(*) from {0}".format(table_name)).collect()[0][0]
-- MAGIC print('No of Rows available in Target table : ',No_rows)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit(No_rows)

-- COMMAND ----------

--show databases;
--use hr;
show tables;
