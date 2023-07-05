# Databricks notebook source
#create delta table

from delta.tables import *
DeltaTable.create(spark).tableName("store")\
.addColumn("store_name","string")\
.addColumn("Address","string")\
.property("description","stores table")\
.location("/FileStore/shared_uploads/manigandan.mj@diggibyte.com/data_folder/store")\
.execute()



# COMMAND ----------

# MAGIC %sql
# MAGIC insert into store values("SJS","Chennai");
# MAGIC insert into store values("JJS","Bnagalore");
# MAGIC
# MAGIC select* from store

# COMMAND ----------

# MAGIC %sql
# MAGIC update store set Address = "Bangalore" where store_name = "JJS";
# MAGIC select * from store

# COMMAND ----------

df = DeltaTable.forPath(spark, "/FileStore/shared_uploads/manigandan.mj@diggibyte.com/data_folder/store")
df_history = df.history()
display(df_history)

# COMMAND ----------

df_delta = spark.read.format('delta').option("timestampAsOf","2023-07-05T17:14:04.000+0000").table("store")
display(df_delta)

# COMMAND ----------

