# Databricks notebook source
# MAGIC %md
# MAGIC ##### Copyright (c) Microsoft Corporation.
# MAGIC ##### Licensed under the MIT license.
# MAGIC 
# MAGIC ###### File: Step02a_Data_Wrangling
# MAGIC ###### Name: Rajdeep Biswas
# MAGIC ###### Date: 06/09/2020

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step02a_Data_Wrangling R notebook
# MAGIC 
# MAGIC This notebook has the following process flow:
# MAGIC   1. Run Step01a_Setup for the Source and sink Configurations and Intialize the spark session.
# MAGIC   2. Use SparkSQL to enrich and curate and load the data from 3 cities in the sink Blob Storage as parquet formatted files

# COMMAND ----------

# MAGIC  %run "./Step01a_Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC Azure Blob storage is a service for storing large amounts of unstructured object data, such as text or binary data. You can use Blob storage to expose data publicly to the world, or to store application data privately. Common uses of Blob storage include:
# MAGIC •	Serving images or documents directly to a browser
# MAGIC •	Storing files for distributed access
# MAGIC •	Streaming video and audio
# MAGIC •	Storing data for backup and restore, disaster recovery, and archiving
# MAGIC •	Storing data for analysis by an on-premises or Azure-hosted service
# MAGIC 
# MAGIC We can read data from public storage accounts without any additional settings. To read data from a private storage account, we need to configure a Shared Key or a Shared Access Signature (SAS).
# MAGIC 
# MAGIC Input widgets allows us to add parameters to your notebooks and dashboards. The widget API consists of calls to create various types of input widgets, remove them, and get bound values.
# MAGIC Widgets are best for:
# MAGIC 
# MAGIC •	Building a notebook or dashboard that is re-executed with different parameters
# MAGIC 
# MAGIC •	Quickly exploring results of a single query with different parameters

# COMMAND ----------

sparkR.conf('fs.azure.sas.safetydata.analyticsdatalakeraj.blob.core.windows.net')

# COMMAND ----------

# MAGIC %md
# MAGIC In this notebook we are going to wrangle and explore the data from 3 cities Chicago, Boston and the city of New York

# COMMAND ----------

#Constructing the source absolute paths
blob_base_path = paste('wasbs://',dbutils.widgets.get("source_blob_container_name"),'@',dbutils.widgets.get("source_blob_account_name"),'.blob.core.windows.net',sep="")
blob_absolute_path_chicago = paste(blob_base_path,"/Safety/Release/city=Chicago",sep="")
blob_absolute_path_boston = paste(blob_base_path,"/Safety/Release/city=Boston",sep="")
blob_absolute_path_newyorkcity = paste(blob_base_path,"/Safety/Release/city=NewYorkCity",sep="")
#print the absolute paths
cat("",blob_absolute_path_chicago,"\n",blob_absolute_path_boston,"\n",blob_absolute_path_newyorkcity)


# COMMAND ----------

# MAGIC %md
# MAGIC The 3-1-1 data in these 3 cities are organized by Azure Open Datasets in parquet format.
# MAGIC 
# MAGIC Apache Parquet is a free and open-source column-oriented data storage format of the Apache Hadoop ecosystem. It is similar to the other columnar-storage file formats available in Hadoop namely RCFile and ORC. It is compatible with most of the data processing frameworks in the Hadoop environment. It provides efficient data compression and encoding schemes with enhanced performance to handle complex data in bulk. 

# COMMAND ----------

#Read the source data in a dataframe for 3 cities
raw_chicago_safety_df <- read.df(blob_absolute_path_chicago, source = "parquet")
raw_boston_safety_df <- read.df(blob_absolute_path_boston, source = "parquet")
raw_newyorkcity_safety_df <- read.df(blob_absolute_path_newyorkcity, source = "parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC Let us explore the 3 datasets

# COMMAND ----------

printSchema(raw_chicago_safety_df)

# COMMAND ----------

printSchema(raw_boston_safety_df)

# COMMAND ----------

printSchema(raw_newyorkcity_safety_df)

# COMMAND ----------

#Lets gauge the scale of the source dataset we are analyzing
cat("","Number of rows for Chicago dataset: ",format(count(raw_chicago_safety_df),big.mark = ","),"\n", "Number of rows for Boston dataset: ",format(count(raw_boston_safety_df),big.mark = ",") ,"\n", "Number of rows for New York City dataset: ",format(count(raw_newyorkcity_safety_df),big.mark = ","))

# COMMAND ----------

#Display the structure of the Chicago DataFrame, including column names, column types, as well as a a small sample of rows.
str(raw_chicago_safety_df)

# COMMAND ----------

#Display the structure of the Boston DataFrame, including column names, column types, as well as a a small sample of rows.
str(raw_boston_safety_df)

# COMMAND ----------

#Display the structure of the New York City DataFrame, including column names, column types, as well as a a small sample of rows.
str(raw_newyorkcity_safety_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC we need the different date components for timeseries analysis

# COMMAND ----------

raw_chicago_safety_df = mutate(raw_chicago_safety_df,date=date_format(raw_chicago_safety_df$dateTime, "MM-dd-yyyy"),
                                  year=year(raw_chicago_safety_df$dateTime),
                                  month=date_format(raw_chicago_safety_df$dateTime, "MMM"),
                                  weekOfMonth=date_format(raw_chicago_safety_df$dateTime, "W"),
                                  dayOfWeek= date_format(raw_chicago_safety_df$dateTime, "E"),
                                  hour= date_format(raw_chicago_safety_df$dateTime, "H")
                                  )

# COMMAND ----------

#Register the DataFrame as a SQL temporary view: raw_chicago_safety_view'
#Registers a DataFrame as a Temporary Table in the SQLContext
registerTempTable(raw_chicago_safety_df, "raw_chicago_safety_view")

# COMMAND ----------

#Display top 10 rows
display(sql('SELECT * FROM raw_chicago_safety_view LIMIT 10'))

# COMMAND ----------

# MAGIC %md
# MAGIC Steps to filter out columns which we do not need

# COMMAND ----------

# Display distinct dataType 
#Since it only has one value "Safety" we can drop it from the enriched dataset
display(sql('SELECT distinct(dataType) FROM raw_chicago_safety_view'))

# COMMAND ----------

# Display distinct dataSubtype 
#Since it only has one value "311_All" we can drop it from the enriched dataset
display(sql('SELECT distinct(dataSubtype) FROM raw_chicago_safety_view'))

# COMMAND ----------

# Display distinct source 
#Since it only has one value "null" we can drop it from the enriched dataset
display(sql('SELECT distinct(source) FROM raw_chicago_safety_view'))

# COMMAND ----------

# Display distinct extendedProperties 
#Since it only has one value null we can drop it from the enriched dataset
display(sql('SELECT distinct(extendedProperties) FROM raw_chicago_safety_view'))

# COMMAND ----------

display(sql('describe raw_chicago_safety_view'))

# COMMAND ----------

enriched_chicago_safety_df = sql('SELECT dateTime,category,subcategory,status,address,latitude,longitude,date,year,month,weekOfMonth,dayOfWeek,hour
               FROM raw_chicago_safety_view
            ')

# COMMAND ----------

#Constructing the sink absolute paths
sink_blob_base_path = paste('wasbs://',dbutils.widgets.get("sink_blob_container_name"),'@',dbutils.widgets.get("sink_blob_account_name"),'.blob.core.windows.net',sep="")
sink_blob_absolute_path_chicago = paste(sink_blob_base_path,"/city311/city=Chicago",sep="")
sink_blob_absolute_path_boston = paste(sink_blob_base_path,"/city311/city=Boston",sep="")
sink_blob_absolute_path_newyorkcity = paste(sink_blob_base_path,"/city311/city=NewYorkCity",sep="")
#print the absolute paths
cat("",sink_blob_absolute_path_chicago,"\n",sink_blob_absolute_path_boston,"\n",sink_blob_absolute_path_newyorkcity)


# COMMAND ----------

write.parquet(enriched_chicago_safety_df, sink_blob_absolute_path_chicago)

# COMMAND ----------

raw_boston_safety_df = mutate(raw_boston_safety_df,date=date_format(raw_boston_safety_df$dateTime, "MM-dd-yyyy"),
                                  year=year(raw_boston_safety_df$dateTime),
                                  month=date_format(raw_boston_safety_df$dateTime, "MMM"),
                                  weekOfMonth=date_format(raw_boston_safety_df$dateTime, "W"),
                                  dayOfWeek= date_format(raw_boston_safety_df$dateTime, "E"),
                                  hour= date_format(raw_boston_safety_df$dateTime, "H")
                                  )

# COMMAND ----------

#Register the DataFrame as a SQL temporary view: raw_boston_safety_view'
#Registers a DataFrame as a Temporary Table in the SQLContext
registerTempTable(raw_boston_safety_df, "raw_boston_safety_view")

# COMMAND ----------

#Display top 10 rows
display(sql('SELECT * FROM raw_boston_safety_view LIMIT 10'))

# COMMAND ----------

enriched_boston_safety_df = sql('SELECT dateTime,category,subcategory,status,address,latitude,longitude,date,year,month,weekOfMonth,dayOfWeek,hour
               FROM raw_boston_safety_view
            ')

# COMMAND ----------

write.parquet(enriched_boston_safety_df, sink_blob_absolute_path_boston)

# COMMAND ----------

raw_newyorkcity_safety_df = mutate(raw_newyorkcity_safety_df,date=date_format(raw_newyorkcity_safety_df$dateTime, "MM-dd-yyyy"),
                                  year=year(raw_newyorkcity_safety_df$dateTime),
                                  month=date_format(raw_newyorkcity_safety_df$dateTime, "MMM"),
                                  weekOfMonth=date_format(raw_newyorkcity_safety_df$dateTime, "W"),
                                  dayOfWeek= date_format(raw_newyorkcity_safety_df$dateTime, "E"),
                                  hour= date_format(raw_newyorkcity_safety_df$dateTime, "H")
                                  )

# COMMAND ----------

#Register the DataFrame as a SQL temporary view: raw_newyorkcity_safety_view'
#Registers a DataFrame as a Temporary Table in the SQLContext
registerTempTable(raw_newyorkcity_safety_df, "raw_newyorkcity_safety_view")

# COMMAND ----------

#Display top 10 rows
display(sql('SELECT * FROM raw_newyorkcity_safety_view LIMIT 10'))

# COMMAND ----------

enriched_newyorkcity_safety_df = sql('SELECT dateTime,category,subcategory,status,address,latitude,longitude,date,year,month,weekOfMonth,dayOfWeek,hour
               FROM raw_newyorkcity_safety_view
            ')

# COMMAND ----------

write.parquet(enriched_newyorkcity_safety_df, sink_blob_absolute_path_newyorkcity)

# COMMAND ----------

# MAGIC  %md
# MAGIC  We wrote the enriched datasets from the three cities in the following place:
# MAGIC  wasbs://safetydata@analyticsdatalakeraj.blob.core.windows.net/city311/city=Chicago 
# MAGIC  wasbs://safetydata@analyticsdatalakeraj.blob.core.windows.net/city311/city=Boston 
# MAGIC  wasbs://safetydata@analyticsdatalakeraj.blob.core.windows.net/city311/city=NewYorkCity
# MAGIC  
# MAGIC  Notice this naturally paritions the data based on city so that we can reason over 3 cities in a distribuuted fashion and make use of partition pruning
