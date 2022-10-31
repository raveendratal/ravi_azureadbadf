# Databricks notebook source
# MAGIC %md 
# MAGIC # DBUTILS  Notebook Commands

# COMMAND ----------

# MAGIC %md #### Run external notebooks
# MAGIC * Notebooks to be run need to be copied to the remote filesystem, e.g. /dbfs/home/<username>/... with /dbfs/ 
# MAGIC   * being the posix pount of the dbfs   filesystem. Notebooks can be copied to remote dbfs with the databricks utility, e.g.
# MAGIC 
# MAGIC * databricks --profile demo fs cp initialize.ipynb /dbfs/home/bernhard/
# MAGIC 
# MAGIC * Since this notebook runs on the remote machine, this cannot be done in this notebook

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

# MAGIC %md #### Calling one notebook into another notebook.
# MAGIC *  `%run  notebook_name` calling one notebook into another notebook using `%run` command

# COMMAND ----------

dbutils.notebook.run("notebookname",3000,{arug:val,arug2:value})

# COMMAND ----------

dbutils.notebook.run("/Shared/Pyspark_Training/Tutorial_4_Joins",30000)

# COMMAND ----------

dbutils.notebook.exit("failed")

# COMMAND ----------

dbutils.notebook.run("../../Tutorial_1_Introduction",60)

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")

# COMMAND ----------

dbutils.notebook.run('./Tutorial_1_Introduction',60)

# COMMAND ----------

#Run with parameters (Variables)
%run ./notebook_name $VAR_1="10" $VAR_2="1"

# COMMAND ----------

dbutils.notebook.exit("stop")

# COMMAND ----------

# MAGIC %run 
# MAGIC dbutils.notebook.run("")

# COMMAND ----------

# MAGIC %run ./Tutorial_1_Introduction

# COMMAND ----------

# MAGIC %md #### Running notebook using dbutils methods.
# MAGIC 
# MAGIC *  `dbutils.notebook.run` command with three parameers, 1) notebook file path, 2) timeout in seconds

# COMMAND ----------

dbutils.notebook.run("Workspace/Shared/Ravi/Read_Write_XML_Files",60)

# COMMAND ----------

# MAGIC %md #### Exit  notebook using dbutils methods.
# MAGIC 
# MAGIC 
# MAGIC *  `dbutils.notebook.exit` using this command we can exit notebook and pass input values to this notebook exit command

# COMMAND ----------

dbutils.notebook.exit("some parameter values.")
#this can be used if we are callign this notebook in Azure ADF Pipelines, 
#we can pass some values through this exit command.
