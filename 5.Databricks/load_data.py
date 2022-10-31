# Databricks notebook source
# MAGIC %run "/Shared/Pyspark_Complete_Tutorial/5.Databricks/data_load" $path="dbfs:/FileStore/tables/emp_pipe.csv" $header="true" $sep ="|" $inferSchema="true" $table_name="emp_pipe"

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_pipe

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/

# COMMAND ----------

# MAGIC %fs head dbfs:/FileStore/tables/emp_pipe.csv

# COMMAND ----------

# MAGIC %run "/Shared/Pyspark_Complete_Tutorial/5.Databricks/libraries"

# COMMAND ----------

# MAGIC %run "/Shared/Pyspark_Complete_Tutorial/5.Databricks/functions"

# COMMAND ----------

add(10,3)





# COMMAND ----------

dbutils.notebook.run("/Shared/Pyspark_Complete_Tutorial/5.Databricks/data_load",6000,{})
