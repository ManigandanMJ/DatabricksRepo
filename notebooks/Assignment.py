# Databricks notebook source
#creating mount point
dbutils.fs.mount(source = 'wasbs://manidbcontainer@manidb1azure.blob.core.windows.net',
mount_point = '/mnt/mntassignment',
extra_configs = {'fs.azure.account.key.manidb1azure.blob.core.windows.net':'X96T9Vc26NOxHA2ZnMomDDDLtPhUeGwNXub8cFQ1cOZgSDJNk5yHdKE/h1hIZjtawWRb2gozVuCH+AStZlT6sw=='}
)

# COMMAND ----------

dbutils.fs.ls('/mnt/mntassignment')

# COMMAND ----------

dbutils.fs.cp('/mnt/mntassignment/raw-data/employee.csv','/mnt/mntassignment/bronze/employee.Parquet')

# COMMAND ----------

dbutils.fs.cp('/mnt/mntassignment/raw-data/employee.json','/mnt/mntassignment/bronze/employee_json.Parquet')

# COMMAND ----------

#added comments
df = spark.read.option('header', True).csv('/mnt/mntassignment/raw-data/employee.csv')
display(df)

# COMMAND ----------

#saving file as parquet
df.write.parquet('/mnt/mntassignment/bronze/employee.parquet')

# COMMAND ----------

#reading the parquet file
employee_df = spark.read.parquet('/mnt/mntassignment/bronze/employee.parquet')

# COMMAND ----------

#raeding the json file
employee_df2 = spark.read.option('multiline', True).json('/mnt/mntassignment/raw-data/employee.json')

# COMMAND ----------

#flattening the file
from pyspark.sql.functions import explode
df_flattened = employee_df2.withColumn("Flattened_project", explode("projects"))

# COMMAND ----------

#writing the parquet file
df_flattened.write.parquet('/mnt/mntassignment/bronze/employee_flattened.parquet')

# COMMAND ----------

#reading from parquet file
employee_df = spark.read.parquet('/mnt/mntassignment/bronze/employee.parquet')
employee_jdf = spark.read.parquet('/mnt/mntassignment/bronze/employee_flattened.parquet')
display(employee_df)
display(employee_jdf)

# COMMAND ----------

#removing the duplicate values
employee_without_dup = employee_df.dropDuplicates()
employee_jdf_without_dup = employee_jdf.dropDuplicates()

# COMMAND ----------

#converting the caplital letter in columns to small letter
emp_df_lowercase_col = employee_without_dup.toDF(*[c.lower() for c in employee_without_dup.columns])
emp_jdf_lowercase_col = employee_jdf_without_dup.toDF(*[c.lower() for c in employee_jdf_without_dup.columns])

# COMMAND ----------

#converting null valuesto 0
from pyspark.sql.functions import col,when
emp_df = emp_df_lowercase_col.withColumn("city",when(col('city') == 'Null',0)\
                                                .when(col('city').isNull(),'0')\
                                                .otherwise(col('city')))\
                                                .na.fill(0,['age'])

# COMMAND ----------

display(emp_df)

# COMMAND ----------

#storing in silver layers
emp_df.write.parquet('/mnt/mntassignment/silver/employee.parquet')
emp_jdf_lowercase_col.write.parquet('/mnt/mntassignment/silver/employee_jdf.parquet')

# COMMAND ----------

#reading from silver layers
emp1 = spark.read.parquet('/mnt/mntassignment/silver/employee.parquet')
emp2 = spark.read.parquet('/mnt/mntassignment/silver/employee_jdf.parquet')

# COMMAND ----------

#joining the two df to single df
combined_df = emp1.join(emp2,["id"],"inner")

# COMMAND ----------

#creating the delta table from dataframes
combined_df.write.format("delta").mode("overwrite").option('path','/mnt/mntassignment/gold').saveAsTable("employee_delta")

# COMMAND ----------

#reading the delta table
delta_df = spark.read.format("delta").load("/mnt/mntassignment/gold")

# COMMAND ----------

#filtering the values and storing in dataframe
filtered_df = delta_df.filter((delta_df.id == "31") | (delta_df.id == "40") | (delta_df.id == "7") | (delta_df.id == "15"))

# COMMAND ----------

