# Databricks notebook source
#To run the notebook from another notebook

print(dbutils.notebook.run('/Users/manigandan.mj@diggibyte.com/Utility_commands',60))


# COMMAND ----------


# To give the values from the widgets created in another notebook
dbutils.notebook.run('/Users/manigandan.mj@diggibyte.com/Utility_commands',60,{'Teams':'IT','Gender':'Male','Skills':'Python','Comments':'Empty..!'})

# The above code will run the Utility_commands notebook and use the parameters we pass in this current notebook and execute Utili_comamnds notebook




# COMMAND ----------

#Sql parameterized notebook with widgets
CREATE WIDGET TEXT comments DEFAULT 'No comments'

#Sql commands to dropdown widget
CREATE WIDGET DROPDOWN gender DEFAULT 'male' CHOICES SELECT 'male'




# COMMAND ----------

# MAGIC %python
# MAGIC #create table from dataframe
# MAGIC data = [(1,"Kunal","Male"),(2,"Jeevan","Male"),(3,"Baegam","Female")]
# MAGIC cols = ["Id","Name","Gender"]
# MAGIC df = spark.createDataFrame(data, cols)
# MAGIC display(df)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC #creating a temp table from dataframe
# MAGIC df.createOrReplaceTempView('user_table')
# MAGIC
# MAGIC %sql
# MAGIC SELECT * FROM user_table
# MAGIC
# MAGIC CREATE WIDGET DROPDOWN Gender DEFAULT 'male' CHOICES SELECT DISTINCT Gender from user_table
# MAGIC
# MAGIC #To remove widget
# MAGIC REMOVE WIDGET Gender
# MAGIC
# MAGIC #To get the particular values from table
# MAGIC SELECT * FROM user_table WHERE Gender = getArgument("Gender")
# MAGIC
# MAGIC

# COMMAND ----------

# Mount fs commands
dbutils.fs.mount(
    source='wasbs://manidbcontainer@manidb1azure.blob.core.windows.net/',
    mount_point = '/mnt/blobstorage',
    extra_configs = {'fs.azure.account.key.manidb1azure.blob.core.windows.net':'X96T9Vc26NOxHA2ZnMomDDDLtPhUeGwNXub8cFQ1cOZgSDJNk5yHdKE/h1hIZjtawWRb2gozVuCH+AStZlT6sw=='}
)



# COMMAND ----------

# using the mount
dbutils.fs.ls('/mnt/blobstorage')

# COMMAND ----------

# To check if  the content is getting copied
dbutils.fs.cp('/mnt/blobstorage/user.csv','/mnt/blobstorage/data_folder/user.csv')

# COMMAND ----------

