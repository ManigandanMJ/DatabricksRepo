#Assignment – 1

-	Load sample CSV into Databricks and create a Data Frame and write it into bronze layer dbfs path as parquet format. 
-	Load sample Json into Databricks and create a Data Frame and flatten column projects and write it into bronze layer dbfs path as 
     parquet format. 
-	Load both the data from bronze layer and Remove duplicate, small case column name, replace null with 0 value, and write it into the 
     silver layer dbfs path as parquet.
-	Load data from silver layer and join the both data frame and store it as a delta table gold layer dbfs path.
-	Now delete the data from the table where id = “31,40,7,15”
-	Restore the table in a previous version.


# Databricks concepts 

    - Practiced on utility commands.
    - Read, Write and again read the created file.
    - Dataframes creation and joining.
    - Mount_point functions.
    - Notebook utilities.
    - Parameterize notebook widgets.
    - widget utilities.
    - Mount and unmount blob storage in notebooks.
