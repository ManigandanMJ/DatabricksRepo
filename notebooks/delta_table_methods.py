# Databricks notebook source
#Creating delta table using create method
from delta.tables import *

DeltaTable.create(spark)\
    .tableName("Products")\
    .addColumn("Prod_name","STRING")\
    .addColumn("Price","INT")\
    .addColumn("Prod_id","STRING")\
    .property("description","Product table")\
    .execute()


# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO Products VALUES("Books",200,"P1");
# MAGIC INSERT INTO Products VALUES("Pen",20,"P2");
# MAGIC INSERT INTO Products VALUES("Paper",50,"P3");
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Products

# COMMAND ----------

#Creating table using createIfNotExists method
from delta.tables import *

DeltaTable.createIfNotExists(spark)\
    .tableName("Users")\
    .addColumn("User_id","INT")\
    .addColumn("Name","STRING")\
    .addColumn("Gender","STRING")\
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO Users VALUES(002,"Kunal","M");
# MAGIC INSERT INTO Users VALUES(003,"Jeevan","M");
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Users

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE Patient (
# MAGIC   pat_id INT,
# MAGIC   name STRING,
# MAGIC   age INT
# MAGIC ) USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE doctor (
# MAGIC   doctor_name STRING,
# MAGIC   Specialist STRING,
# MAGIC   availablity STRING
# MAGIC ) USING DELTA
# MAGIC LOCATION '/dbfs/FileStore/shared_uploads/manigandan.mj@diggibyte.com/data_folder'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO doctor VALUES("Mani","Heart","yes");
# MAGIC INSERT INTO doctor VALUES("Jeevan","Head","No");

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM doctor

# COMMAND ----------

