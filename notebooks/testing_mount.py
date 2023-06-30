# Databricks notebook source
#creating new mount_points by 2 ways
dbutils.fs.mount(source = 'wasbs://manidbcontainer@manidb1azure.blob.core.windows.net',
                 mount_point = '/mnt/blobstoragetest',
                 extra_configs = {'fs.azure.account.key.manidb1azure.blob.core.windows.net':'X96T9Vc26NOxHA2ZnMomDDDLtPhUeGwNXub8cFQ1cOZgSDJNk5yHdKE/h1hIZjtawWRb2gozVuCH+AStZlT6sw=='}

)

# COMMAND ----------

#list of files in container using mount_point created
dbutils.fs.ls('/mnt/blobstoragetest')

# COMMAND ----------

# Creating mount_point using SAS key
dbutils.fs.mount(source = 'wasbs://manidbcontainer@manidb1azure.blob.core.windows.net',
                 mount_point = '/mnt/blobstoragesaskey',
                 extra_configs = {'fs.azure.sas.manidbcontainer.manidb1azure.blob.core.windows.net':'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2023-06-30T17:01:04Z&st=2023-06-30T09:01:04Z&spr=https&sig=KbA%2FbTFtdkyt9Ztiqr6yx3US0oDUaT7I%2BDwW6tGaN5o%3D'}

)

# COMMAND ----------

dbutils.fs.ls('/mnt/blobstoragesaskey')

# COMMAND ----------

#Reading the csv file and creating df using mount_point path
df = spark.read.csv('/mnt/blobstoragesaskey/user.csv', header = True)
df.show()


# COMMAND ----------

#unmount the mount_point
#dbutils.fs.unmount('/mnt/blobstoragetest')
#based on above code we cant access mount_point for any changes next


# COMMAND ----------

#To check the list of mounts (lists the number of mount_points available on cluster)
dbutils.fs.mounts()

#Refresh mounts cmd (all the mount_points will be updated with latest data)
dbutils.fs.refreshMounts()


# COMMAND ----------

dbutils.fs.ls('/mnt/blobstoragetest')

# COMMAND ----------

dbutils.fs.mount(source = 'wasbs://manidbcontainer@manidb1azure.blob.core.windows.net/data_folder',
                 mount_point = '/mnt/blobstorage1',
                 extra_configs = {'fs.azure.account.key.manidb1azure.blob.core.windows.net':'X96T9Vc26NOxHA2ZnMomDDDLtPhUeGwNXub8cFQ1cOZgSDJNk5yHdKE/h1hIZjtawWRb2gozVuCH+AStZlT6sw=='}

)

# COMMAND ----------

dbutils.fs.ls('/mnt/blobstorage1')

# COMMAND ----------

#Update Mount
dbutils.fs.updateMount(source = 'wasbs://manidbcontainer@manidb1azure.blob.core.windows.net',
                 mount_point = '/mnt/blobstorage1',
                 extra_configs = {'fs.azure.account.key.manidb1azure.blob.core.windows.net':'X96T9Vc26NOxHA2ZnMomDDDLtPhUeGwNXub8cFQ1cOZgSDJNk5yHdKE/h1hIZjtawWRb2gozVuCH+AStZlT6sw=='}

)

# COMMAND ----------

#updated mount_point
dbutils.fs.ls('/mnt/blobstorage1')