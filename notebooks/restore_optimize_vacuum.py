# Databricks notebook source
from delta.tables import *
DeltaTable.create(spark)\
    .tableName("Books")\
    .addColumn("book_id","INT")\
    .addColumn("name","STRING")\
    .addColumn("price","INT")\
    .property("description","Bokks_details")\
    .location("/FileStore/shared_uploads/manigandan.mj@diggibyte.com/data_folder/Books")\
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into Books values(101,"ethics",200);
# MAGIC insert into Books values(102,"friends",1000);
# MAGIC insert into Books values(103,"connect me",2000);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Books

# COMMAND ----------

book_instance = DeltaTable.forPath(spark,"/FileStore/shared_uploads/manigandan.mj@diggibyte.com/data_folder/Books")

# COMMAND ----------

display(book_instance.history())

# COMMAND ----------

book_instance.restoreToVersion(2)

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select * from Books

# COMMAND ----------

book_instance.restoreToTimestamp('2023-07-06T06:38:25.000+0000')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Books

# COMMAND ----------

from delta.tables import *
DeltaTable.create(spark)\
    .tableName("student")\
    .addColumn("stu_id","INT")\
    .addColumn("name","STRING")\
    .addColumn("class","INT")\
    .property("description","Bokks_details")\
    .location("/FileStore/shared_uploads/manigandan.mj@diggibyte.com/data_folder/student")\
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into student values(1231,"Kamlesh",12);
# MAGIC insert into student values(1232,"Fazil",11);
# MAGIC insert into student values(1234,"Anand",10);

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into student values(1235,"kiran",12);
# MAGIC insert into student values(1236,"kamal",11);
# MAGIC insert into student values(1237,"gokul",10);

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from student where stu_id = 11

# COMMAND ----------

# MAGIC %sql
# MAGIC update student set name = "Guna" where name = "kamal"

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize student

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history student

# COMMAND ----------

ls /dbfs/dbfs/FileStore/shared_uploads/manigandan.mj@diggibyte.com/data_folder/

# COMMAND ----------

# MAGIC %sql 
# MAGIC optimize student zorder by name

# COMMAND ----------

