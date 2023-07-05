# Databricks notebook source
#Creating delta table 
from delta.tables import *

DeltaTable.create(spark)\
    .tableName("school_details")\
        .addColumn("school_name","STRING")\
        .addColumn("address","STRING")\
        .property("description","school table")\
        .location("/FileStore/shared_uploads/manigandan.mj@diggibyte.com/data_folder/merge_log")\
        .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO school_details VALUES("SJS","tvm")

# COMMAND ----------


from pyspark.sql.types import *
from pyspark.sql.functions import *

schema = StructType([StructField("school_name",StringType(),True),
                     StructField("address",StringType(),True)

])
data = [("srms","chennai"),("Jeevan","bangalore")]
school_df = spark.createDataFrame(data = data,schema = schema)



# COMMAND ----------

display(school_df)
school_df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO school_details AS target
# MAGIC USING source_view  AS source
# MAGIC   ON target.school_name = source.school_name
# MAGIC   WHEN MATCHED
# MAGIC THEN UPDATE SET
# MAGIC   target.school_name = source.school_name,
# MAGIC   target.address = source.address
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (school_name,address) VALUES (school_name,address)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM school_details

# COMMAND ----------

#delta table to dataframe
delta_df = DeltaTable.forPath(spark, "/FileStore/shared_uploads/manigandan.mj@diggibyte.com/data_folder/merge_log")
last_opration_df = delta_df.history(1)
display(last_opration_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table audit_log(opertaion STRING,
# MAGIC                       updated_time timestamp,
# MAGIC                       user_name string,
# MAGIC                       notebook_name string,
# MAGIC                       numTargetRowsUpdated int,
# MAGIC                       numTargetRowsInserted int,
# MAGIC                       numTargetRowsDeleted int)
# MAGIC                       

# COMMAND ----------

explode_df = last_opration_df.select(last_opration_df.operation,explode(last_opration_df.operationMetrics))
#explode_df_cast = explode_df.select(explode_df.operation,explode_df.key,explode_df.value.cast('int'))

# COMMAND ----------

display(explode_df)
#display(explode_df_cast)

# COMMAND ----------

pivot_df = explode_df_cast.groupBy("operation").pivot("key").sum("value")

# COMMAND ----------

display(pivot_df)

# COMMAND ----------

pivot_select = pivot_df.select(pivot_df.operation,pivot_df.numTargetRowsUpdated,pivot_df.numTargetRowsInserted,pivot_df.numTargetRowsDeleted)

# COMMAND ----------

display(pivot_select)

# COMMAND ----------

audit_df = pivot_select.withColumn("user_name",lit(dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get())).withColumn("notebook_name",lit(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())).withColumn("updated_time",lit(current_timestamp()))
display(audit_df)

# COMMAND ----------

