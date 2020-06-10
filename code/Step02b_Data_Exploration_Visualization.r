# Databricks notebook source
# MAGIC %md
# MAGIC ##### Copyright (c) Microsoft Corporation.
# MAGIC ##### Licensed under the MIT license.
# MAGIC 
# MAGIC ###### File: Step02b_Data_Exploration_Visualization
# MAGIC ###### Date: 06/09/2020

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step02b_Data_Exploration_Visualization R notebook
# MAGIC 
# MAGIC This notebook has the following process flow:
# MAGIC   1. Run Step01a_Setup for the Source and sink Configurations and Intialize the spark session.
# MAGIC   2. Libraries and R HTML widgets setup.
# MAGIC   3. Explore and Visualize the data from 3 cities Chicago, Boston and the city of New York

# COMMAND ----------

# MAGIC  %run "./Step01a_Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC Libraries

# COMMAND ----------

# library(SparkR) #Spark R is already loaded from the Step01a_Setup notebook
library(ggplot2)
library(magrittr)
library(leaflet)
library(htmltools)
library(htmlwidgets)

# COMMAND ----------

# MAGIC %md
# MAGIC Refer: https://docs.microsoft.com/en-us/azure/databricks/_static/notebooks/azure/htmlwidgets-azure.html
# MAGIC 
# MAGIC Below steps shows how you can get R HTML widgets working in Azure Databricks notebooks. The setup has two steps:
# MAGIC 
# MAGIC Installing pandoc, a Linux package that is used by HTML widgets to generate HTML Changing one function within HTML Widgets package to make it seemless work in Azure Databricks. Make sure you use correct URL of your Azure Databricks environment. Both steps can be automated using init scripts so that when your cluster launches it installs pandoc and updates R HTMLwidgets package automatically.

# COMMAND ----------

# MAGIC %md
# MAGIC Installing HTML Widgets linux dependency on the driver

# COMMAND ----------

# MAGIC %sh 
# MAGIC apt-get --yes install pandoc

# COMMAND ----------

## Replace <region> with the region of your Azure Databricks Account URL
## Make sure you use HTTPS "?o=<workspace-id>"
databricksURL <- "https://<region>.azuredatabricks.net/files/rwidgets/"

# COMMAND ----------

#Fix HTML Widgets package to work in Azure Databricks Notebooks
### Replace <workspace-id> with the workspace ID of your Azure Databricks Account URL
db_html_print <- function(x, ..., view = interactive()) {
  fileName <- paste(tempfile(), ".html", sep="")
  htmlwidgets::saveWidget(x, file = fileName)
  
  randomFileName = paste0(floor(runif(1, 0, 10^12)), ".html")
  baseDir <- "/dbfs/FileStore/rwidgets/"
  dir.create(baseDir)
  internalFile = paste0(baseDir, randomFileName)
  externalFile = paste0(databricksURL, randomFileName, "?o=<workspace-id>")
  system(paste("cp", fileName, internalFile))
  displayHTML(externalFile)
}
R.utils::reassignInPackage("print.htmlwidget", pkgName = "htmlwidgets", value = db_html_print)

# COMMAND ----------

# MAGIC %md
# MAGIC The 3-1-1 data in these 3 cities are organized by Azure Open Datasets in parquet format.
# MAGIC 
# MAGIC Apache Parquet is a free and open-source column-oriented data storage format of the Apache Hadoop ecosystem. It is similar to the other columnar-storage file formats available in Hadoop namely RCFile and ORC. It is compatible with most of the data processing frameworks in the Hadoop environment. It provides efficient data compression and encoding schemes with enhanced performance to handle complex data in bulk. 

# COMMAND ----------

# MAGIC %md
# MAGIC Let us explore the 3 datasets

# COMMAND ----------

#Constructing the enriched absolute paths
#Notice here we are using all 3 cities
sink_blob_base_path = paste('wasbs://',dbutils.widgets.get("sink_blob_container_name"),'@',dbutils.widgets.get("sink_blob_account_name"),'.blob.core.windows.net',sep="")

#Optionally read individual cities
#sink_blob_absolute_path_chicago = paste(sink_blob_base_path,"/city311/city=Chicago",sep="")
#sink_blob_absolute_path_boston = paste(sink_blob_base_path,"/city311/city=Boston",sep="")
#sink_blob_absolute_path_newyorkcity = paste(sink_blob_base_path,"/city311/city=NewYorkCity",sep="")

#Read all 3 cities
enriched_blob_absolute_path_3cities = paste(sink_blob_base_path,"/city311",sep="")
#print the absolute paths
cat(enriched_blob_absolute_path_3cities)


# COMMAND ----------

#Read the enriched data in a dataframe for 3 cities
enriched_3cities_safety_df <- read.df(enriched_blob_absolute_path_3cities, source = "parquet")

# COMMAND ----------

#Read the enriched data in a dataframe for 3 cities
enriched_chicago_safety_df <- filter(enriched_3cities_safety_df, enriched_3cities_safety_df$city == "Chicago")
enriched_boston_safety_df <- filter(enriched_3cities_safety_df, enriched_3cities_safety_df$city == "Boston")
enriched_newyorkcity_safety_df <- filter(enriched_3cities_safety_df, enriched_3cities_safety_df$city == "NewYorkCity")

# COMMAND ----------

#Lets gauge the scale of the source dataset we are analyzing
cat("","Number of rows for Chicago dataset: ",format(count(enriched_chicago_safety_df),big.mark = ","),"\n", "Number of rows for Boston dataset: ",format(count(enriched_boston_safety_df),big.mark = ",") ,"\n", "Number of rows for New York City dataset: ",format(count(enriched_newyorkcity_safety_df),big.mark = ","))

# COMMAND ----------

printSchema(enriched_3cities_safety_df)

# COMMAND ----------

str(enriched_3cities_safety_df)

# COMMAND ----------

#Register the DataFrame as a SQL temporary view: enriched_3cities_safety_view'
#Registers a DataFrame as a Temporary Table in the SQLContext
registerTempTable(enriched_3cities_safety_df, "enriched_3cities_safety_view")

# COMMAND ----------

#Display top 10 rows
display(sql('SELECT * FROM enriched_3cities_safety_view LIMIT 10'))

# COMMAND ----------

# MAGIC %md
# MAGIC Lets us visualize the categories in Chicago city

# COMMAND ----------

chicago_category_grouped_df = sql('SELECT category, count, ROW_NUMBER() OVER (ORDER BY count DESC) as rank
             FROM (
               SELECT category, COUNT(*) as count 
               FROM enriched_3cities_safety_view
               WHERE city = "Chicago"
               GROUP BY category)
            ')


# COMMAND ----------

str(chicago_category_grouped_df)

# COMMAND ----------

#Get the top 30 category for plotting
chicago_category_grouped_df_top30 = collect(filter(chicago_category_grouped_df,"rank <= 30"))

# COMMAND ----------

display(chicago_category_grouped_df_top30)

# COMMAND ----------

# MAGIC %md
# MAGIC Plot the top 30 incidents reported in Chicago

# COMMAND ----------

ggplot(chicago_category_grouped_df_top30, aes(x = reorder(category,-count,sum), y = count, fill = count)) + geom_col() +
  labs(y = "Count of Incidents",x="Category of Incidents", title = paste0("Safety Incidents, ","Chicago")) +
  theme(axis.title = element_text(size = 16), title = element_text(size = 16), axis.text.y = element_text(size = 6), legend.position = "none") +
  scale_fill_gradient(low = "blue", high = "red") +
  coord_flip()

# COMMAND ----------

# MAGIC %md
# MAGIC We might have to remove few chart toppers as they are dominant for example the information only calls

# COMMAND ----------

# MAGIC %md
# MAGIC Now we will plot the bottom 30 occuring incidents.

# COMMAND ----------

take(arrange(chicago_category_grouped_df, "rank", decreasing = TRUE),30)

# COMMAND ----------

#Get the bottom 30 category for plotting
chicago_category_grouped_df_bottom30 = take(arrange(chicago_category_grouped_df, "rank", decreasing = TRUE),30)

# COMMAND ----------

ggplot(chicago_category_grouped_df_bottom30, aes(x = reorder(category,-count,sum), y = count, fill = count)) + geom_col() +
  labs(y = "Count of Incidents",x="Category of Incidents", title = paste0("Safety Incidents, ","Chicago Bottom 30")) +
  theme(axis.title = element_text(size = 16), title = element_text(size = 16), axis.text.y = element_text(size = 6), legend.position = "none") +
  scale_fill_gradient(low = "green", high = "pink") +
  coord_flip()

# COMMAND ----------

# MAGIC %md
# MAGIC There is a stark contrast on the number of cases from the bottom and top 30

# COMMAND ----------

# MAGIC %md
# MAGIC Let us plot the year wise cases in Chicago city

# COMMAND ----------

chicago_year_grouped_df = collect(sql('
               SELECT year as yr, COUNT(*) as count 
               FROM enriched_3cities_safety_view
               WHERE city = "Chicago" and year >= 2011
               GROUP BY year'))

# COMMAND ----------

head(chicago_year_grouped_df)


# COMMAND ----------

ggplot(chicago_year_grouped_df, aes(x = yr, y = count)) +
  geom_line(color = "RED", size = 0.5) +
  scale_x_continuous(breaks=seq(2011, 2020, 1)) +
  labs(x = "Year of Safety Incident", y = "Number of Safety Incidents", title = "Yearly Safety Incidents in Chicago from 2011 – 2020")

# COMMAND ----------

# MAGIC %md 
# MAGIC We will display safety incident locations across the 3 cities using leaflet.
# MAGIC For the purpose of this visualtion we will choose 10k rows from each of the 3 cities

# COMMAND ----------

year_grouped_df_3cities_10k = collect(sql('select * from 
(select * from (SELECT dateTime,category,subcategory,latitude,longitude,address
               FROM enriched_3cities_safety_view
               WHERE city = "Chicago" and year=2019 ) limit 10000) union
(select * from (SELECT dateTime,category,subcategory,latitude,longitude,address 
               FROM enriched_3cities_safety_view
               WHERE city = "Boston" and year=2019 ) limit 10000) union
(select * from (SELECT dateTime,category,subcategory,latitude,longitude,address 
               FROM enriched_3cities_safety_view
               WHERE city = "NewYorkCity" and year=2019 ) limit 10000) '))

# COMMAND ----------

year_grouped_df_3cities_10k$popup <- paste("<b>Category: </b>", year_grouped_df_3cities_10k$category,
                    "<br>", "<b>SubCategory: </b>", year_grouped_df_3cities_10k$subcategory,                      
                    "<br>", "<b>DateTime: </b>", year_grouped_df_3cities_10k$dateTime,
                    "<br>", "<b>Address: </b>", year_grouped_df_3cities_10k$address,
                    "<br>", "<b>Longitude: </b>", year_grouped_df_3cities_10k$longitude,
                    "<br>", "<b>Latitude: </b>", year_grouped_df_3cities_10k$latitude)

# COMMAND ----------

str(year_grouped_df_3cities_10k)

# COMMAND ----------

# MAGIC %md
# MAGIC The dynamic leaffet html is attached separately

# COMMAND ----------

category_maps = leaflet(year_grouped_df_3cities_10k, width = "100%") %>% addTiles() %>%
  addTiles(group = "OSM (default)") %>%
  addProviderTiles(provider = "Esri.WorldStreetMap",group = "World StreetMap") %>%
  addProviderTiles(provider = "Esri.WorldImagery",group = "World Imagery") %>%
  addMarkers(lng = ~longitude, lat = ~latitude, popup = year_grouped_df_3cities_10k$popup, clusterOptions = markerClusterOptions()) %>%
  addLayersControl(
    baseGroups = c("OSM (default)","World StreetMap", "World Imagery"),
    options = layersControlOptions(collapsed = FALSE)
  )
#category_maps

# COMMAND ----------

yearly_count_3cities = collect(sql('SELECT year,city,count(*) as cnt
               FROM enriched_3cities_safety_view
               GROUP BY city,year'))

# COMMAND ----------

str(yearly_count_3cities)

# COMMAND ----------

# MAGIC %md
# MAGIC Changes Over Time - Volume of All Safety Calls

# COMMAND ----------

ggplot(yearly_count_3cities, aes(x = year, y = cnt, color = city)) + geom_line() + geom_point() +
  labs(y = "Yearly Count of All Safety Calls")


# COMMAND ----------

# MAGIC %md
# MAGIC Changes Over Time - Volume of Specific Safety Calls

# COMMAND ----------

yearly_count_3cities_graffiti = collect(sql('SELECT year,city,count(*) as cnt
               FROM enriched_3cities_safety_view 
               WHERE lower(category) like "%graffiti%" OR lower(subcategory) like "%graffiti%"
               GROUP BY city,year'))

# COMMAND ----------

str(yearly_count_3cities_graffiti)

# COMMAND ----------

ggplot(yearly_count_3cities_graffiti, aes(x = year, y = cnt, color = city)) + geom_line() + geom_point() +
scale_x_continuous(breaks=seq(2010, 2020, 1)) +
  labs(title = paste0("Yearly count of graffiti calls"))
