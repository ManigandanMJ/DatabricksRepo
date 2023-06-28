# Databricks notebook source
#create dataframe

user_data = [(1,"mani"), (2,"kunal"), (3,"vikas")]
user_schema = (["id", "name"])
user_df = spark.createDataFrame(user_data, user_schema)
display(user_df)


# COMMAND ----------

#dbutils data_utilities -  gives summarize of df
dbutils.data.summarize(user_df)

# COMMAND ----------

def read_file_df():
    user_df = spark.read.option('header', True).option('inferSchema',True).option('delimiter', ",").csv("dbfs:/FileStore/shared_uploads/manigandan.mj@diggibyte.com/user.csv")
    return user_df

def df_write_file(user_df):
    user_write_csv = user_df.write.option('header', True).option('delimiter', ",").csv("/FileStore/shared_uploads/manigandan.mj@diggibyte.com/data_folder1")
    return user_write_csv

def read_from_csv():
    output_df = spark.read.option('header', True).csv("dbfs:/FileStore/shared_uploads/manigandan.mj@diggibyte.com/data_folder1/part-00000-tid-6377811476414518929-ea2c19ed-adfc-4c05-8303-3980aec15da5-139-1-c000.csv")
    return output_df


user_df = read_file_df()
user_df.display()
write_csv = df_write_file(user_df)
read_from_csv().display()




# COMMAND ----------

#File system utilities
dbutils.fs.cp('FileStore/shared_uploads/manigandan.mj@diggibyte.com/user_src.txt','FileStore/shared_uploads/manigandan.mj@diggibyte.com/new_user_src.txt')

# #To read the file
dbutils.fs.head('FileStore/shared_uploads/manigandan.mj@diggibyte.com/new_user_src.txt',5)

# #To create a folder
dbutils.fs.mkdirs('FileStore/shared_uploads/manigandan.mj@diggibyte.com/data_folder')

#To remove the folder
#dbutils.fs.rm('FileStore/data_folder')

#To remove the file
dbutils.fs.rm('FileStore/shared_uploads/manigandan.mj@diggibyte.com/new_user_src.txt')


#To list the directory files
dbutils.fs.ls('FileStore/shared_uploads/')

#To move the file and copy
dbutils.fs.cp('FileStore/shared_uploads/manigandan.mj@diggibyte.com/user_src.txt','FileStore/shared_uploads/manigandan.mj@diggibyte.com/data_folder/moved_user_src.txt')
dbutils.fs.mv('FileStore/shared_uploads/manigandan.mj@diggibyte.com/data_folder/moved_user_src.txt','FileStore/shared_uploads/manigandan.mj@diggibyte.com/')

#To add data (put) to file
dbutils.fs.put('FileStore/shared_uploads/manigandan.mj@diggibyte.com/moved_user_src.txt',"104,kumal,20",True)





# COMMAND ----------

#widgets_utilities combobox 
dbutils.widgets.combobox(name='Teams',defaultValue='IT',choices=['IT','Admin','Finance'])

#Dropdown
dbutils.widgets.dropdown(name='Gender',defaultValue='None',choices=['Male','Female','other','None'])

#Multi select
dbutils.widgets.multiselect(name='Skills',defaultValue='computer basics',choices=['Java','Python','Sql','computer basics'])

#Textbox
dbutils.widgets.text(name='Comments',defaultValue='No comments')



# COMMAND ----------

