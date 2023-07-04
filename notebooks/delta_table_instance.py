# Databricks notebook source
#Creating delta table 
from delta.tables import *

DeltaTable.create(spark)\
    .tableName("fruits")\
        .addColumn("fruit_name","STRING")\
        .addColumn("quantity","STRING")\
        .addColumn("price","INT")\
        .property("description","fruits table")\
        .location("/FileStore/shared_uploads/manigandan.mj@diggibyte.com/data_folder/delta_table_instances")\
        .execute()


# COMMAND ----------

# MAGIC %sql
# MAGIC /*inserting values to delta table*/
# MAGIC INSERT INTO fruits VALUES("Cherry","1kg",200);
# MAGIC INSERT INTO fruits VALUES("DRY FRUITS","1kg",800);

# COMMAND ----------

fruit_instance = DeltaTable.forPath(spark, "/FileStore/shared_uploads/manigandan.mj@diggibyte.com/data_folder/delta_table_instances")

# COMMAND ----------

display(fruit_instance.toDF())

# COMMAND ----------

#deleting value using instance
fruit_instance.delete("price > 200")

# COMMAND ----------

# MAGIC %sql
# MAGIC /* changes done using instance will affect the table */
# MAGIC SELECT * FROM fruits

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY fruits

# COMMAND ----------

display(fruit_instance.history())

# COMMAND ----------

display(spark.sql("select * from fruits"))

# COMMAND ----------

from pyspark.sql.types import *
fruit_schema = StructType([
    StructField("fruit_name",StringType(),True),
    StructField("quantity",StringType(),True),
    StructField("price",IntegerType(),True)
])
fruit_data = [("Jack","2kg",100),
             ("Banana","1kg",50)]

fruit_df = spark.createDataFrame(data = fruit_data,schema = fruit_schema)
display(fruit_df)

# COMMAND ----------

fruit_df.write.format("delta").mode("append").saveAsTable("fruits")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fruits

# COMMAND ----------

from pyspark.sql.types import *
fruit_schema = StructType([
    StructField("fruit_name",StringType(),True),
    StructField("quantity",StringType(),True),
    StructField("price",IntegerType(),True)
])
fruit_data = [("Grapes","1kg",100),
             ("Orange","1kg",50)]

fruit_df2 = spark.createDataFrame(data = fruit_data,schema = fruit_schema)
display(fruit_df2)

# COMMAND ----------

fruit_df2.write.insertInto('fruits',overwrite = False)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM fruits

# COMMAND ----------

fruit_df2.createOrReplaceTempView("data_delta")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from data_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into fruits select * from data_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fruits

# COMMAND ----------


spark.sql("insert into fruits select * from data_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fruits

# COMMAND ----------

