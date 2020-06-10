# Databricks notebook source
# MAGIC %md
# MAGIC ##### Copyright (c) Microsoft Corporation.
# MAGIC ##### Licensed under the MIT license.
# MAGIC 
# MAGIC ###### File: Step03b_Anomaly_Detection
# MAGIC ###### Name: Rajdeep Biswas
# MAGIC ###### Date: 06/09/2020

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step03b_Anomaly_Detection R notebook
# MAGIC 
# MAGIC This notebook has the following process flow:
# MAGIC   1. Run Step01a_Setup for the Source and sink Configurations and Intialize the spark session.
# MAGIC   2. Load Libraries.
# MAGIC   3. Time series anomaly detection for the 311 safety data from 3 cities Chicago, Boston and the city of New York

# COMMAND ----------

# MAGIC  %run "./Step01a_Setup"

# COMMAND ----------

Libraries

# COMMAND ----------

# library(SparkR) #Spark R is already loaded from the Step01a_Setup notebook
library(anomalize)
library(tidyverse)

# COMMAND ----------

# MAGIC %md
# MAGIC Get the enriched data and register the temporary table

# COMMAND ----------

#Constructing the enriched absolute paths
#Notice here we are using all 3 cities
sink_blob_base_path = paste('wasbs://',dbutils.widgets.get("sink_blob_container_name"),'@',dbutils.widgets.get("sink_blob_account_name"),'.blob.core.windows.net',sep="")
enriched_blob_absolute_path_3cities = paste(sink_blob_base_path,"/city311",sep="")
#print the absolute paths
cat(enriched_blob_absolute_path_3cities)

# COMMAND ----------

#Read the enriched data in a dataframe for 3 cities
enriched_3cities_safety_df <- read.df(enriched_blob_absolute_path_3cities, source = "parquet")

# COMMAND ----------

#Register the DataFrame as a SQL temporary view: enriched_3cities_safety_view'
#Registers a DataFrame as a Temporary Table in the SQLContext
registerTempTable(enriched_3cities_safety_df, "enriched_3cities_safety_view")

# COMMAND ----------

monthly_count_chicago_pothole_2014_2019 = SparkR::collect(SparkR::sql('select to_date(concat(year,"-",month,"-","01"),"yyyy-MMM-dd") as startDayOfMonth,cnt  from (SELECT year,month, city,count(*) as cnt
               FROM enriched_3cities_safety_view 
               WHERE city="Chicago" and (lower(category) like "%pothole%" OR lower(subcategory) like "%pothole%") and year > 2013 and year < 2020
               GROUP BY year,month, city) order by startDayOfMonth') )

# COMMAND ----------

head(monthly_count_chicago_pothole_2014_2019)

# COMMAND ----------

monthly_count_chicago_pothole_2014_2019_tib = as.tibble(monthly_count_chicago_pothole_2014_2019)

# COMMAND ----------

head(monthly_count_chicago_pothole_2014_2019_tib)

# COMMAND ----------

monthly_count_chicago_pothole_2014_2019_tib %>% 
  time_decompose(cnt, method = "stl", frequency = "auto", trend = "auto") %>%
  anomalize(remainder, method = "gesd", alpha = 0.05, max_anoms = 0.2) %>%
  plot_anomaly_decomposition()

# COMMAND ----------

# MAGIC %md
# MAGIC Anomaly Detection and Plotting the detected anomalies are almost similar to what we saw above with Time Series Decomposition. It’s just that decomposed components after anomaly detection are recomposed back with time_recompose() and plotted with plot_anomalies() . The package itself automatically takes care of a lot of parameter setting like index, frequency and trend, making it easier to run anomaly detection out of the box with less prior expertise in the same domain.

# COMMAND ----------

monthly_count_chicago_pothole_2014_2019_tib %>% 
  time_decompose(cnt) %>%
  anomalize(remainder) %>%
  time_recompose() %>%
  plot_anomalies(time_recomposed = TRUE, ncol = 3, alpha_dots = 0.5)

# COMMAND ----------

# MAGIC %md
# MAGIC If you are interested in extracting the actual datapoints which are anomalies, the following code could be used:

# COMMAND ----------

monthly_count_chicago_pothole_2014_2019_tib %>% 
  time_decompose(cnt) %>%
  anomalize(remainder) %>%
  time_recompose() %>%
  filter(anomaly == 'Yes') 

# COMMAND ----------

# MAGIC %md
# MAGIC If we get a bit more granular with breaking the count weekly on the start day of the week

# COMMAND ----------

str(enriched_3cities_safety_df)

# COMMAND ----------

weekly_count_chicago_pothole_2014_2019 = SparkR::collect(SparkR::sql('SELECT startDayOfWeek,cnt FROM (SELECT year,month, weekOfMonth,  min(to_date(date,"MM-dd-yyyy")) as startDayOfWeek, city,count(*) as cnt
               FROM enriched_3cities_safety_view 
               WHERE city="Chicago" and (lower(category) like "%pothole%" OR lower(subcategory) like "%pothole%") and year > 2013 and year < 2020 and date is not null
               GROUP BY year,month, weekOfMonth, city) order by startDayOfWeek'))

# COMMAND ----------

head(weekly_count_chicago_pothole_2014_2019)

# COMMAND ----------

weekly_count_chicago_pothole_2014_2019_tib = as.tibble(weekly_count_chicago_pothole_2014_2019)

# COMMAND ----------

head(weekly_count_chicago_pothole_2014_2019_tib)

# COMMAND ----------

# MAGIC %md
# MAGIC anomalize has three main functions:
# MAGIC 
# MAGIC time_decompose(): Separates the time series into seasonal, trend, and remainder components
# MAGIC 
# MAGIC anomalize(): Applies anomaly detection methods to the remainder component.
# MAGIC 
# MAGIC time_recompose(): Calculates limits that separate the “normal” data from the anomalies!

# COMMAND ----------

weekly_count_chicago_pothole_2014_2019_tib %>% 
  time_decompose(cnt, method = "stl", frequency = "auto", trend = "auto") %>%
  anomalize(remainder, method = "gesd", alpha = 0.05, max_anoms = 0.2) %>%
  plot_anomaly_decomposition() +
 labs(title = "Decomposition of Anomalized Chicago Weekly Pothole Repair Complaints") +
    xlab("Year (Data collected as weekly aggregate)")

# COMMAND ----------

weekly_count_chicago_pothole_2014_2019_tib %>% 
  time_decompose(cnt) %>%
  anomalize(remainder) %>%
  time_recompose() %>%
  plot_anomalies(time_recomposed = TRUE, ncol = 3, alpha_dots = 0.5)+
    labs(title = "Chicago Weekly Pothole Repair Complaints Anomalies") 

# COMMAND ----------

weekly_count_chicago_pothole_2014_2019_tib %>%
    # Data Manipulation / Anomaly Detection
    time_decompose(cnt, method = "stl") %>%
    anomalize(remainder, method = "iqr") %>%
    time_recompose() %>%
    # Anomaly Visualization
    plot_anomalies(time_recomposed = TRUE, ncol = 3, alpha_dots = 0.5) +
    labs(title = "Tidyverse Chicago Weekly Pothole Repair Complaints Anomalies", subtitle = "STL + IQR Methods")   +
    xlab("Year (Data collected as weekly aggregate)")

# COMMAND ----------

#plot_anomaly_decomposition() for visualizing the inner workings of how algorithm detects anomalies in the “remainder”.
weekly_count_chicago_pothole_2014_2019_tib %>%
    ungroup() %>%
    time_decompose(cnt) %>%
    anomalize(remainder) %>%
    plot_anomaly_decomposition() +
    labs(title = "Decomposition of Anomalized Chicago Weekly Pothole Repair Complaints") +
    xlab("Year (Data collected as weekly aggregate)")

# COMMAND ----------

weekly_count_chicago_pothole_2014_2019_tib %>% 
  time_decompose(cnt) %>%
  anomalize(remainder) %>%
  time_recompose() %>%
  filter(anomaly == 'Yes') 

# COMMAND ----------

# MAGIC %md
# MAGIC observation from the data: Why there was bump in potholes repair complaints in 2018 February?
# MAGIC From the records 2018 Jan-Feb had a harsh winter and flooding. Also snow, ice and moisture all contribute to potholes but a cycle of freezing temperatures followed by higher temperatures helps the formation of potholes. and that explains the anamoly : 
# MAGIC 
# MAGIC https://abc7chicago.com/chicago-weather-potholes-heavy-rain-flood-watch/3112763/
# MAGIC https://digitaledition.chicagotribune.com/tribune/article_popover.aspx?guid=0815ff4c-6db6-4166-848c-eed12b08a702

# COMMAND ----------

# MAGIC %md
# MAGIC Going by the theme of our resrach i.e whether the 3 cities are related let us find the anamolies in New York City and Boston also.
# MAGIC We observe both the cities during the early 2018 had a rise in cases of pothole complaints. We also see from the data that the trends and anomalies in pothole complaints in Boston and New York City are very similar which can be attributed to their proximity and climate similarities.

# COMMAND ----------

weekly_count_boston_pothole_2014_2019 = SparkR::collect(SparkR::sql('SELECT startDayOfWeek,cnt FROM (SELECT year,month, weekOfMonth,  min(to_date(date,"MM-dd-yyyy")) as startDayOfWeek, city,count(*) as cnt
               FROM enriched_3cities_safety_view 
               WHERE city="Boston" and (lower(category) like "%pothole%" OR lower(subcategory) like "%pothole%") and year > 2013 and year < 2020 and date is not null
               GROUP BY year,month, weekOfMonth, city) order by startDayOfWeek'))

# COMMAND ----------

weekly_count_boston_pothole_2014_2019_tib = as.tibble(weekly_count_boston_pothole_2014_2019)

# COMMAND ----------

weekly_count_boston_pothole_2014_2019_tib %>% 
  time_decompose(cnt) %>%
  anomalize(remainder) %>%
  time_recompose() %>%
  filter(anomaly == 'Yes') 

# COMMAND ----------

weekly_count_newyorkcity_pothole_2014_2019 = SparkR::collect(SparkR::sql('SELECT startDayOfWeek,cnt FROM (SELECT year,month, weekOfMonth,  min(to_date(date,"MM-dd-yyyy")) as startDayOfWeek, city,count(*) as cnt
               FROM enriched_3cities_safety_view 
               WHERE city="NewYorkCity" and (lower(category) like "%pothole%" OR lower(subcategory) like "%pothole%") and year > 2013 and year < 2020 and date is not null
               GROUP BY year,month, weekOfMonth, city) order by startDayOfWeek'))

# COMMAND ----------

weekly_count_newyorkcity_pothole_2014_2019_tib = as.tibble(weekly_count_newyorkcity_pothole_2014_2019)

# COMMAND ----------

weekly_count_newyorkcity_pothole_2014_2019_tib %>% 
  time_decompose(cnt) %>%
  anomalize(remainder) %>%
  time_recompose() %>%
  filter(anomaly == 'Yes') 
