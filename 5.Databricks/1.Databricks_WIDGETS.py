# Databricks notebook source
# MAGIC %md
# MAGIC #### dbutils.widgets  usage 
# MAGIC #### Widget types
# MAGIC ###### There are 4 types of widgets:
# MAGIC 
# MAGIC * `text`: Input a value in a text box.
# MAGIC * `dropdown`: Select a value from a list of provided values.
# MAGIC * `combobox`: Combination of text and dropdown. Select a value from a provided list or input one in the text box.
# MAGIC * `multiselect`: Select one or more values from a list of provided values.
# MAGIC * `Widget dropdowns` and `text boxes` appear immediately following the notebook toolbar.
# MAGIC 
# MAGIC * `dbutils.widgets.text And dbutils.widgets.get` we are using to create variables and passing values. 
# MAGIC * `text(name: String, defaultValue: String, label: String) `: void -> Creates a text input widget with a given name and default value
# MAGIC * `get(name: String)` : String -> Retrieves current value of an input widget
# MAGIC * `combobox(name: String, defaultValue: String, choices: Seq, label: String)`: void -> Creates a combobox input widget with a 
# MAGIC   * given name, default value and choices
# MAGIC * `dropdown(name: String, defaultValue: String, choices: Seq, label: String)`: void -> Creates a dropdown input widget a 
# MAGIC    * with given name, default value and choices

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `text(name: String, defaultValue: String, label: String)`
# MAGIC * Creates a text input widget with a given name and default value

# COMMAND ----------

dbutils.widgets.text("sample_variable", "text")
print(dbutils.widgets.get("sample_variable"))

# COMMAND ----------

sample_v = dbutils.widgets.get("sample_variable")
print(sample_v)

# COMMAND ----------

print(dbutils.widgets.get("foo"))
print(getArgument('foo'))

# COMMAND ----------

dbutils.widgets.text("P_NAME","A","label_name")
# 1st parameter is Variable Name
# 2nd Parameter default value
# 3rd Parameter is display name for input Parameter
V_NAME = dbutils.widgets.get("P_NAME")
print(V_NAME)

# COMMAND ----------

V_NAME = dbutils.widgets.get("P_NAME")
print(V_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating `COMBOBOX` variable using `dbutils.widgets.combobox`
# MAGIC * 1st parameter Parameter Name
# MAGIC * 2nd Parameter Default Value
# MAGIC * 3rd list (list of multiple values)
# MAGIC * 4th parameter Label name which will display in notebook header (optional)

# COMMAND ----------

dbutils.widgets.combobox("P_COMBOBOX","ESWAR",["VAMSI","LAKSHMI","RAJA"]) 
V_COMBO = dbutils.widgets.get("P_COMBOBOX")
print(V_COMBO)

# COMMAND ----------

V_COMBO = dbutils.widgets.get("P_COMBOBOX")
print(V_COMBO)

# COMMAND ----------

# MAGIC %md
# MAGIC #### DROPDOWN 
# MAGIC * dropdown(name: String, defaultValue: String, choices: Seq, label: String)
# MAGIC * Creates a dropdown input widget a with given name, default value and choices

# COMMAND ----------

dbutils.widgets.dropdown("P_DROPDOWN", "1", ["1","2","3"],"SELECT_DROPDOWN")
V_DROPDOWN = dbutils.widgets.get("P_DROPDOWN")
print(V_DROPDOWN)

# COMMAND ----------

#getArgument("")
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.dropdown("P_DROPDOWN2","2",["1","2","3"]) 
V_dropdown = dbutils.widgets.get("P_DROPDOWN")
print(V_dropdown)

# COMMAND ----------

dbutils.widgets.dropdown("X123", "1", [str(x) for x in range(1, 10)])

dbutils.widgets.dropdown("1", "1", [str(x) for x in range(1, 10)], "hello this is a widget")

# COMMAND ----------

# MAGIC %md
# MAGIC #### MultiSelect
# MAGIC * multiselect(name: String, defaultValue: String, choices: Seq, label: String): 
# MAGIC * Creates a multiselect input widget with a given name, default value and choices

# COMMAND ----------

dbutils.widgets.multiselect("MSelect", "RAJA",["ESWAR",'RAJA',"VAMSI","LAKSHMI","1","2","3"],"MultiSelect")
print(dbutils.widgets.get("MSelect").split(","))

# COMMAND ----------

getArgument('MSelect').split(",")


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Remove Individual Widgets or All Widgets... using `remove`  or  `removeAll`

# COMMAND ----------

#Remove All widgets 
#dbutils.widgets.remove('MSelect')
#dbutils.widgets.removeAll()

# COMMAND ----------

# Remove selected Widgets...
#dbutils.widgets.remove("MSelect")


# COMMAND ----------

#dbutils.fs.rm("dbfs:/babynames.csv")

# COMMAND ----------

import urllib3
response = urllib3.PoolManager().request('GET', 'http://health.data.ny.gov/api/views/myeu-hzra/rows.csv')
csvfile = response.data.decode("utf-8")
dbutils.fs.put("dbfs:/babynames.csv", csvfile)

# COMMAND ----------


babynames = spark.read.csv("dbfs:/babynames.csv",header=True,inferSchema=True)
babynames.createOrReplaceTempView("babynames_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from babynames_table

# COMMAND ----------

#v_year = dbutils.widgets.get('year')
display(babynames.filter(babynames['Year']==getArgument('year')))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from babynames_table

# COMMAND ----------

years = spark.sql("select distinct(Year) from babynames_table").rdd.map(lambda row : row[0]).collect()
years.sort()
years

# COMMAND ----------

dbutils.widgets.dropdown("year",'2014', [str(x) for x in years],"year")
display(babynames.filter(babynames.Year == getArgument("year")))


# COMMAND ----------

b=int(getArgument('year'))
b

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT DISTINCT year FROM babynames_table

# COMMAND ----------

# dbutils.widgets.get("sql_year")
#getArgument("sql_year")

# COMMAND ----------

# MAGIC %md #### Widgets in SQL
# MAGIC * The API to create widgets in SQL is slightly different but as powerful as the APIs for the other languages. 
# MAGIC * The following is an example of creating a text input widget.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET DROPDOWN sql_year DEFAULT "2014" CHOICES SELECT DISTINCT year FROM babynames_table

# COMMAND ----------

# MAGIC %sql
# MAGIC remove widget sql_year

# COMMAND ----------

# MAGIC %md #### getArgument

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from babynames_table where  year  = getArgument('sql_year');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT y DEFAULT "10"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating DROPDOWN widgets variables in SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS diamonds USING CSV OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET DROPDOWN cuts DEFAULT "Good" CHOICES SELECT DISTINCT cut FROM diamonds

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM diamonds WHERE cut LIKE '%$cuts%'

# COMMAND ----------

# MAGIC %md ##### Remove WIDGET using SQL

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC REMOVE WIDGET sql_year

# COMMAND ----------

emp_csv = spark.read.csv("/FileStore/tables/emp.csv",header=True,inferSchema=True)

# COMMAND ----------

emp_csv.createGlobalTempView("emp_csv")

# COMMAND ----------

emp_csv.printSchema()

# COMMAND ----------

from pyspark.sql.functions import split
a=getArgument('P_DEPTNO').split(",")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Selecting `MULTIPLE` values and applying in filter clause using split(',')

# COMMAND ----------

dept_list = ('10','20','30','40')
dbutils.widgets.multiselect("P_DEPTNO",'10',dept_list)
#Selecting multiple values and applying multiple values in filter (Where)
print(getArgument('P_DEPTNO').split(","))

# COMMAND ----------

emp_csv.filter(emp_csv.DEPTNO.isin(getArgument('P_DEPTNO').split(","))).show()

# COMMAND ----------

#dbutils.widgets.removeAll()

# COMMAND ----------

dept_no = tuple(getArgument('P_DEPTNO').split(','))
dept_no

# COMMAND ----------

display(spark.sql(f'select * from global_temp.emp_csv where deptno in {dept_no}'))

# COMMAND ----------

dbutils.widgets.multiselect("X", "1", [str(x) for x in range(1, 10)])

# COMMAND ----------

print(getArgument('X').split(','))

# COMMAND ----------

# MAGIC %sql
# MAGIC select getArgument("X")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET MULTISELECT D_NO DEFAULT "10" CHOICES SELECT  distinct DEPTNO FROM global_temp.emp_csv
# MAGIC --SELECT DISTINCT DEPTNO FROM global_temp.emp_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC select getArgument("D_NO")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select  * from global_temp.emp_csv where deptno in (getArgument("D_NO")) ;
