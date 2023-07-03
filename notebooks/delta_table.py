# Databricks notebook source
#Delta lake table using createOrReplace method
from delta.tables import *
DeltaTable.createOrReplace(spark).tableName("employee").\
addColumn("Name","string").\
addColumn("Dept","string").\
property("description","Employee details").\
location("/FileStore/shared_uploads/manigandan.mj@diggibyte.com/data_folder/employee_delta").\
execute()

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /FileStore/shared_uploads/manigandan.mj@diggibyte.com/data_folder/employee_delta/_delta_log
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC head /FileStore/shared_uploads/manigandan.mj@diggibyte.com/data_folder/employee_delta/_delta_log/00000000000000000000.json

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from employee
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into employee values("Arivu","Admin");
# MAGIC insert into employee values("jeeva","IT");
# MAGIC insert into employee values("Prathvi","Admin");
# MAGIC insert into employee values("kiran","Admin");
# MAGIC insert into employee values("Buvan","IT");

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/shared_uploads/manigandan.mj@diggibyte.com/data_folder/employee_delta/_delta_log

# COMMAND ----------

# MAGIC %fs
# MAGIC head /FileStore/shared_uploads/manigandan.mj@diggibyte.com/data_folder/employee_delta/_delta_log/00000000000000000005.json

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into employee values("Jeevith","Admin");
# MAGIC insert into employee values("Prem","IT");
# MAGIC insert into employee values("Ravi","Admin");
# MAGIC insert into employee values("Andrew","Admin");

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee

# COMMAND ----------

# MAGIC %sql 
# MAGIC delete from employee where Name = "Ravi"

# COMMAND ----------

display(spark.read.format('parquet').load("/FileStore/shared_uploads/manigandan.mj@diggibyte.com/data_folder/employee_delta/_delta_log/00000000000000000010.checkpoint.parquet"))
