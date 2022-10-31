# Databricks notebook source
dbutils.widgets.multiselect("deptno","10",["10","20","30","40","50","60","70","80"])

# COMMAND ----------

v_deptno = dbutils.widgets.get("deptno").split(",")
print('Selected Value from widgets : ',v_deptno)

# COMMAND ----------

df = spark.sql("select * from emp")

# COMMAND ----------

print('filter values : ',v_deptno)

# COMMAND ----------

display(df.filter(df["DEPTNO"].isin(v_deptno)))

# COMMAND ----------

display(df.filter(df["DEPTNO"].isin(v_deptno)))
