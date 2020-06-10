# Databricks notebook source
# MAGIC %md
# MAGIC ##### Copyright (c) Microsoft Corporation.
# MAGIC ##### Licensed under the MIT license.
# MAGIC 
# MAGIC ###### File: Step01a_Setup
# MAGIC ###### Date: 06/09/2020

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step01a_Setup R notebook
# MAGIC 
# MAGIC This notebook has the following process flow:
# MAGIC   1. Source and sink Configurations. We are using databricks dbutils widget here.
# MAGIC   2. For the sink account name and sas token we are using Azure Key Vault to secure the credentials.
# MAGIC      
# MAGIC      Ref: https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes#--create-an-azure-key-vault-backed-secret-scope 
# MAGIC       and https://docs.microsoft.com/en-us/azure/databricks/security/secrets/example-secret-workflow
# MAGIC   3. Load the SparkR Library
# MAGIC   4. Intialize the spark session

# COMMAND ----------

#Source configuration
#dbutils.widgets.removeAll()
dbutils.widgets.text("source_blob_account_name","azureopendatastorage","Source Blob Account Name")
dbutils.widgets.text("source_blob_container_name","citydatacontainer","Source Blob Container Name")
dbutils.widgets.text("source_blob_sas_token","source_blob_sas_token_value","Source Blob SAS Token")
#source_blob_sas_token value :  ?st=2019-02-26T02%3A34%3A32Z&se=2119-02-27T02%3A34%3A00Z&sp=rl&sv=2018-03-28&sr=c&sig=XlJVWA7fMXCSxCKqJm8psMOh0W4h7cSYO28coRqF2fs%3D

#Sink configuration
dbutils.widgets.text("sink_blob_account_name","analyticsdatalakeraj","Sink Blob Account Name")
dbutils.widgets.text("sink_blob_container_name","safetydata","Sink Blob Container Name")
#dbutils.widgets.text("sink_blob_sas_token","sink_blob_sas_token_value","Sink Blob SAS Token") #Use the secure credetial here rather than widget or clear text values
sink_blob_sas_token =dbutils.secrets.get(scope="tale_of_3_cities_secrets", key="sink-blob-sas-token")

# COMMAND ----------

#Libraries
library(SparkR)
#library(tidyverse)
#library(anomalize)
#library(ggplot2)

# COMMAND ----------

#Get the value for source_storage_conf
(source_storage_conf=paste('fs.azure.sas.',dbutils.widgets.get("source_blob_container_name"),'.',dbutils.widgets.get("source_blob_account_name"),'.blob.core.windows.net',sep=""))

# COMMAND ----------

#Get the value for sink_storage_conf
(sink_storage_conf=paste('fs.azure.sas.',dbutils.widgets.get("sink_blob_container_name"),'.',dbutils.widgets.get("sink_blob_account_name"),'.blob.core.windows.net',sep=""))

# COMMAND ----------

# initialize Spark session
sparkR.session(
  sparkConfig = list(
    'fs.azure.sas.citydatacontainer.azureopendatastorage.blob.core.windows.net' = dbutils.widgets.get("source_blob_sas_token"),
    'fs.azure.sas.safetydata.analyticsdatalakeraj.blob.core.windows.net' = sink_blob_sas_token 
  )
)

# COMMAND ----------

#Quick test #Notice how the SAS token value is secured from being displayed as open text
sparkR.conf('fs.azure.sas.safetydata.analyticsdatalakeraj.blob.core.windows.net')

# COMMAND ----------

# MAGIC %md
# MAGIC Once an account access key or a SAS is set up in the Spark conf in the notebook, we can use standard Spark and Databricks APIs to read from the storage account.
