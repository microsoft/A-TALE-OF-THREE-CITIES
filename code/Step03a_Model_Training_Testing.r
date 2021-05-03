# Databricks notebook source
# MAGIC %md
# MAGIC ##### Copyright (c) Microsoft Corporation.
# MAGIC ##### Licensed under the MIT license.
# MAGIC 
# MAGIC ###### File: Step03a_Model_Training_Testing
# MAGIC ###### Date: 06/09/2020

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step03a_Model_Training_Testing R notebook
# MAGIC 
# MAGIC This notebook has the following process flow:
# MAGIC   1. Run Step01a_Setup for the Source and sink Configurations and Intialize the spark session.
# MAGIC   2. Load Libraries.
# MAGIC   3. Time series analysis and forecasting for the 311 safety data from 3 cities Chicago, Boston and the city of New York

# COMMAND ----------

# MAGIC  %run "./Step01a_Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC Libraries

# COMMAND ----------

# library(SparkR) #Spark R is already loaded from the Step01a_Setup notebook
library(ggplot2)
library(forecast)
library(ggfortify)
library(fpp2)

# COMMAND ----------

# MAGIC %md
# MAGIC The data from 3 cities Chicago, Boston and the city of New York is enriched in our previous step.

# COMMAND ----------

# MAGIC %md
# MAGIC The 3-1-1 data in these 3 cities are organized in parquet format.
# MAGIC 
# MAGIC Apache Parquet is a free and open-source column-oriented data storage format of the Apache Hadoop ecosystem. It is similar to the other columnar-storage file formats available in Hadoop namely RCFile and ORC. It is compatible with most of the data processing frameworks in the Hadoop environment. It provides efficient data compression and encoding schemes with enhanced performance to handle complex data in bulk. 

# COMMAND ----------

# MAGIC %md
# MAGIC Let us explore the 3 datasets

# COMMAND ----------

#Constructing the enriched absolute paths
#Notice here we are using all 3 cities
sink_blob_base_path = paste('wasbs://',dbutils.widgets.get("sink_blob_container_name"),'@',dbutils.widgets.get("sink_blob_account_name"),'.blob.core.windows.net',sep="")
#sink_blob_absolute_path_chicago = paste(sink_blob_base_path,"/city311/city=Chicago",sep="")
#sink_blob_absolute_path_boston = paste(sink_blob_base_path,"/city311/city=Boston",sep="")
#sink_blob_absolute_path_newyorkcity = paste(sink_blob_base_path,"/city311/city=NewYorkCity",sep="")
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
# MAGIC Lets us get the top 30 categories in each city

# COMMAND ----------

allcities_category_grouped_df_top30 = sql('select * from (SELECT city, category, count, ROW_NUMBER() OVER (PARTITION BY city ORDER BY count DESC) as rank
             FROM (
               SELECT city, category, COUNT(*) as count 
               FROM enriched_3cities_safety_view
               GROUP BY city, category)) where rank <=30
            ')


# COMMAND ----------

display(allcities_category_grouped_df_top30)

# COMMAND ----------

allcities_subcategory_grouped_df_top30 = sql('select * from (SELECT city, subcategory, count, ROW_NUMBER() OVER (PARTITION BY city ORDER BY count DESC) as rank
             FROM (
               SELECT city, subcategory, COUNT(*) as count 
               FROM enriched_3cities_safety_view
               GROUP BY city, subcategory)) where rank <=30
            ')


# COMMAND ----------

display(allcities_subcategory_grouped_df_top30)

# COMMAND ----------

# MAGIC %md
# MAGIC Point to note here is Pothole gets called out in category column of Chicago and Subcategory column of New York City and Boston.
# MAGIC 
# MAGIC Pothole facts from wiki (https://en.wikipedia.org/wiki/Pothole#Costs_to_the_public): The American Automobile Association estimated in the five years prior to 2016 that 16 million drivers in the United States have suffered damage from potholes to their vehicle including tire punctures, bent wheels, and damaged suspensions with a cost of $3 billion a year. In India, 3,000 people per year are killed in accidents involving potholes. Britain has estimated that the cost of fixing all roads with potholes in the country would cost £12 billion.
# MAGIC 
# MAGIC We will focus on the pothole data from the 3 cities however the techniques can be applied across other categories as well.

# COMMAND ----------

# MAGIC %md
# MAGIC Get all the pothole complaints by city and date

# COMMAND ----------

yearly_count_3cities_pothole = collect(sql('SELECT year,city,count(*) as cnt
               FROM enriched_3cities_safety_view 
               WHERE lower(category) like "%pothole%" OR lower(subcategory) like "%pothole%"
               GROUP BY city,year'))
			   

# COMMAND ----------

ggplot(yearly_count_3cities_pothole, aes(x = year, y = cnt, color = city)) + geom_line() + geom_point() +
scale_x_continuous(breaks=seq(2010, 2020, 1)) +
  labs(title = paste0("Yearly count of pothole repair calls"))	

# COMMAND ----------

weekly_count_3cities_pothole = collect(sql('select * from (SELECT year,month, weekOfMonth, city,count(*) as cnt
               FROM enriched_3cities_safety_view 
               WHERE lower(category) like "%pothole%" OR lower(subcategory) like "%pothole%"
               GROUP BY year,month, weekOfMonth, city) order by year,month,weekOfMonth') )

# COMMAND ----------

head(weekly_count_3cities_pothole)

# COMMAND ----------

ggplot(weekly_count_3cities_pothole, aes(x = year, y = cnt, color = city)) + geom_line() + geom_point() +
scale_x_continuous(breaks=seq(2010, 2020, 1)) +
  labs(title = paste0("Weekly count of pothole repair calls"))	

# COMMAND ----------

# MAGIC %md
# MAGIC A time series can be thought of as a vector or matrix of numbers along with some information about what times those numbers were recorded. This information is stored in a ts object in R. 
# MAGIC ts(data, start, frequency, ...)

# COMMAND ----------

monthly_count_3cities_pothole = collect(sql('select to_date(concat(year,"-",month,"-","01"),"yyyy-MMM-dd") as startDayOfMonth,year,month, city,cnt  from (SELECT year,month, city,count(*) as cnt
               FROM enriched_3cities_safety_view 
               WHERE lower(category) like "%pothole%" OR lower(subcategory) like "%pothole%"
               GROUP BY year,month, city) order by startDayOfMonth') )

# COMMAND ----------

head(monthly_count_3cities_pothole)

# COMMAND ----------

monthly_count_chicago_pothole_2014_2019 = collect(sql('select to_date(concat(year,"-",month,"-","01"),"yyyy-MMM-dd") as startDayOfMonth,cnt  from (SELECT year,month, city,count(*) as cnt
               FROM enriched_3cities_safety_view 
               WHERE city="Chicago" and (lower(category) like "%pothole%" OR lower(subcategory) like "%pothole%") and year > 2013 and year < 2020
               GROUP BY year,month, city) order by startDayOfMonth') )

# COMMAND ----------

head(monthly_count_chicago_pothole_2014_2019)

# COMMAND ----------

# MAGIC %md
# MAGIC Convert into ts object

# COMMAND ----------

monthly_count_chicago_pothole_2014_2019_ts = ts(monthly_count_chicago_pothole_2014_2019[, 2], start = c(2014, 1), frequency = 12)

# COMMAND ----------

head(monthly_count_chicago_pothole_2014_2019_ts)

# COMMAND ----------

class(monthly_count_chicago_pothole_2014_2019_ts)

# COMMAND ----------

frequency(monthly_count_chicago_pothole_2014_2019)

# COMMAND ----------

str(monthly_count_chicago_pothole_2014_2019_ts)

# COMMAND ----------

# MAGIC %md
# MAGIC Graphs enable you to visualize many features of the data, including patterns, unusual observations, changes over time, and relationships between variables. Just as the type of data determines which forecasting method to use, it also determines which graphs are appropriate.
# MAGIC You can use the autoplot() function to produce a time plot of the data with or without facets, or panels that display different subsets of data:

# COMMAND ----------

ggplot2::autoplot(monthly_count_chicago_pothole_2014_2019_ts) +
  ggtitle("Monthly Chicago Pothole Incidents") +
  xlab("Year") +
  ylab("Count")

# COMMAND ----------

monthly_count_allcities_pothole_2014_2019 = collect(sql('select to_date(concat(year,"-",month,"-","01"),"yyyy-MMM-dd") as startDayOfMonth,cnt,city  from (SELECT year,month, city,count(*) as cnt
               FROM enriched_3cities_safety_view 
               WHERE (lower(category) like "%pothole%" OR lower(subcategory) like "%pothole%") and year > 2013 and year < 2020
               GROUP BY year,month, city) order by startDayOfMonth') )


# COMMAND ----------

class(monthly_count_allcities_pothole_2014_2019)

# COMMAND ----------

monthly_count_allcities_pothole_2014_2019_df = sql('select to_date(concat(year,"-",month,"-","01"),"yyyy-MMM-dd") as startDayOfMonth,cnt,city  from (SELECT year,month, city,count(*) as cnt
               FROM enriched_3cities_safety_view 
               WHERE (lower(category) like "%pothole%" OR lower(subcategory) like "%pothole%") and year > 2013 and year < 2020
               GROUP BY year,month, city) order by startDayOfMonth') 


# COMMAND ----------

class(monthly_count_allcities_pothole_2014_2019_df)

# COMMAND ----------

monthly_count_allcities_pothole_2014_2019_pivot <- collect(sum(pivot(groupBy(monthly_count_allcities_pothole_2014_2019_df, "startDayOfMonth"), "city"), "cnt"))

# COMMAND ----------

class(monthly_count_allcities_pothole_2014_2019_pivot)

# COMMAND ----------

head(monthly_count_allcities_pothole_2014_2019_pivot)

# COMMAND ----------

monthly_count_allcities_pothole_2014_2019_pivot_ts = ts(monthly_count_allcities_pothole_2014_2019_pivot[, 2:4], start = c(2014, 1), frequency = 12)	

# COMMAND ----------

ggplot2::autoplot(monthly_count_allcities_pothole_2014_2019_pivot_ts, facets = TRUE) +
  ggtitle("Monthly 3 cities Pothole Incidents") +
  xlab("Year") +
  ylab("Count") +
 scale_x_continuous(breaks=seq(2014, 2019, 1)) 

# COMMAND ----------

ggplot2::autoplot(monthly_count_allcities_pothole_2014_2019_pivot_ts, facets = FALSE) +
  ggtitle("Monthly 3 cities Pothole Incidents") +
  xlab("Year") +
  ylab("Count") +
 scale_x_continuous(breaks=seq(2014, 2019, 1)) 

# COMMAND ----------

frequency(monthly_count_allcities_pothole_2014_2019_pivot_ts)

# COMMAND ----------

# MAGIC %md
# MAGIC Interesting observations:
# MAGIC Can the uptick of pothole repairs in the 3 cities during the first half of the yar be attributed to harsh winters?
# MAGIC Can the budget and contract of workers for pothole repair be alloted and spent following the trend?

# COMMAND ----------

# MAGIC %md
# MAGIC Let us look at Chicago in more details

# COMMAND ----------

monthly_count_Chicago_pothole_2014_2019_df = sql('SELECT year, cnt, month  from (SELECT year,month, city,count(*) as cnt
               FROM enriched_3cities_safety_view 
               WHERE city="Chicago" and (lower(category) like "%pothole%" OR lower(subcategory) like "%pothole%") and year > 2013 and year < 2020
               GROUP BY year,month, city) order by year') 

# COMMAND ----------

monthly_count_Chicago_2014_2019_pivot <- collect(sum(pivot(groupBy(monthly_count_Chicago_pothole_2014_2019_df, "year"), "month"), "cnt"))


# COMMAND ----------

monthly_count_Chicago_2014_2019_pivot

# COMMAND ----------

monthly_count_Chicago_2014_2019_pivot_ts = ts(monthly_count_Chicago_2014_2019_pivot[, 2:13], start = c(2014, 1), frequency = 1)	

# COMMAND ----------

monthly_count_Chicago_2014_2019_pivot_ts

# COMMAND ----------

monthly_count_Chicago_pothole_2014_2019_df_1 = collect(sql('SELECT to_date(concat(year,"-",month,"-","01"),"yyyy-MMM-dd") as startDayOfMonth, cnt  from (SELECT year,month, city,count(*) as cnt
               FROM enriched_3cities_safety_view 
               WHERE city="Chicago" and (lower(category) like "%pothole%" OR lower(subcategory) like "%pothole%") and year > 2013 and year < 2020
               GROUP BY year,month, city) order by startDayOfMonth'))

# COMMAND ----------

monthly_count_Chicago_pothole_2014_2019_df_1_ts = ts(monthly_count_Chicago_pothole_2014_2019_df_1[, 2], start = c(2014, 1), frequency = 12)	

# COMMAND ----------

ggseasonplot(monthly_count_Chicago_pothole_2014_2019_df_1_ts, main="Seasonal plot broken by year for pothole repairs in Chicago", ylab = "Count")

# COMMAND ----------

# Produce a polar coordinate season plot for the a10 data
ggseasonplot(monthly_count_Chicago_pothole_2014_2019_df_1_ts, main="Polar coordinate season plot broken by year for pothole repairs in Chicago", ylab = "Count", polar = TRUE)

# COMMAND ----------

ggsubseriesplot(monthly_count_Chicago_pothole_2014_2019_df_1_ts, main="Subseries plot broken by month for pothole repairs in Chicago", ylab = "Count")

# COMMAND ----------

# MAGIC %md
# MAGIC It clearly shows that Feb, Mar and Apr have a uptick in pothole repair cases

# COMMAND ----------

# MAGIC %md
# MAGIC The correlations associated with the lag plots form what is called the autocorrelation function (ACF). The ggAcf() function produces ACF plots.

# COMMAND ----------

# Create an ACF plot of the Chicago data
ggAcf(monthly_count_Chicago_pothole_2014_2019_df_1_ts)

# COMMAND ----------

# MAGIC %md
# MAGIC White noise is a term that describes purely random data. We can conduct a Ljung-Box test using the function below to confirm the randomness of a series; a p-value greater than 0.05 suggests that the data are not significantly different from white noise.

# COMMAND ----------

# Plot the original series
autoplot(monthly_count_Chicago_pothole_2014_2019_df_1_ts)

# Plot the differenced series
autoplot(diff(monthly_count_Chicago_pothole_2014_2019_df_1_ts))

# ACF of the differenced series
ggAcf(diff(monthly_count_Chicago_pothole_2014_2019_df_1_ts))

# Ljung-Box test of the differenced series
Box.test(diff(monthly_count_Chicago_pothole_2014_2019_df_1_ts), lag = 10, type = "Ljung")

# COMMAND ----------

monthly_count_Chicago_pothole_2014_2019_df_1_ts

# COMMAND ----------

# MAGIC %md
# MAGIC A forecast is the mean or median of simulated futures of a time series.
# MAGIC 
# MAGIC The very simplest forecasting method is to use the most recent observation; this is called a naive forecast and can be implemented in a namesake function. This is the best that can be done for many time series including most stock price data, and even if it is not a good forecasting method, it provides a useful benchmark for other forecasting methods.
# MAGIC 
# MAGIC For seasonal data, a related idea is to use the corresponding season from the last year of data. For example, if you want to forecast the sales volume for next March, you would use the sales volume from the previous March. This is implemented in the snaive() function, meaning, seasonal naive.
# MAGIC 
# MAGIC For both forecasting methods, you can set the second argument h, which specifies the number of values you want to forecast; as shown in the code below, they have different default values. The resulting output is an object of class forecast. This is the core class of objects in the forecast package, and there are many functions for dealing with them including summary() and autoplot().

# COMMAND ----------

# Use naive() to forecast the Chicago pothole series
fc_monthly_count_Chicago_pothole_2014_2019 <- naive(monthly_count_Chicago_pothole_2014_2019_df_1_ts, h = 12)


# COMMAND ----------

#Plot forecasts
autoplot(fc_monthly_count_Chicago_pothole_2014_2019) +
  ggtitle("Forecast of 2020 Pothole Incidents") +
  xlab("Year") +
  ylab("Count") 

# COMMAND ----------

#summarize the forecasts
summary(fc_monthly_count_Chicago_pothole_2014_2019)

# COMMAND ----------

# Use snaive() to forecast the ausbeer series
fcs_monthly_count_Chicago_pothole_2014_2019 <- snaive(monthly_count_Chicago_pothole_2014_2019_df_1_ts, h = 12)

# COMMAND ----------

#Plot forecasts
autoplot(fcs_monthly_count_Chicago_pothole_2014_2019) +
  ggtitle("Seasonal Naive Forecast of 2020 Pothole Incidents in Chicago") +
  xlab("Year") +
  ylab("Count") 

# COMMAND ----------

#summarize the forecasts
summary(fcs_monthly_count_Chicago_pothole_2014_2019)

# COMMAND ----------

# MAGIC %md
# MAGIC When applying a forecasting method, it is important to always check that the residuals are well-behaved (i.e., no outliers or patterns) and resemble white noise. The prediction intervals are computed assuming that the residuals are also normally distributed.

# COMMAND ----------

checkresiduals(fcs_monthly_count_Chicago_pothole_2014_2019)

# COMMAND ----------

monthly_count_Chicago_pothole_2014_2019_df_1_ts

# COMMAND ----------

# Create the training data as train
train <- subset(monthly_count_Chicago_pothole_2014_2019_df_1_ts, end = 48)

# COMMAND ----------

train

# COMMAND ----------

# Compute seasonal naive forecasts and save to naive_fc
naive_fc <- snaive(train, h = 108)

# Compute mean forecasts and save to mean_fc
mean_fc <- meanf(train, h = 108)

# Use accuracy() to compute RMSE statistics
print(accuracy(naive_fc, monthly_count_Chicago_pothole_2014_2019_df_1_ts))
print(accuracy(mean_fc, monthly_count_Chicago_pothole_2014_2019_df_1_ts))


# COMMAND ----------

# Create three training series omitting the last 1, 2, and 3 years
train1 <- window(monthly_count_Chicago_pothole_2014_2019_df_1_ts, end = c(2014, 12))
train2 <- window(monthly_count_Chicago_pothole_2014_2019_df_1_ts, end = c(2015, 12))
train3 <- window(monthly_count_Chicago_pothole_2014_2019_df_1_ts, end = c(2016, 12))

# Produce forecasts using snaive()
fc1 <- snaive(train1, h = 4)
fc2 <- snaive(train2, h = 4)
fc3 <- snaive(train3, h = 4)

# Use accuracy() to compare the MAPE of each series
print(accuracy(fc1, monthly_count_Chicago_pothole_2014_2019_df_1_ts)["Test set", "MAPE"])
print(accuracy(fc2, monthly_count_Chicago_pothole_2014_2019_df_1_ts)["Test set", "MAPE"])
print(accuracy(fc3, monthly_count_Chicago_pothole_2014_2019_df_1_ts)["Test set", "MAPE"])

# COMMAND ----------

fcses <- ses(train3, h = 12)
fcnaive <- snaive(train3, h = 12)
fcholt <- holt(train3, h = 12)
fchw <- hw(train3, seasonal = "multiplicative", h = 12)
print(accuracy(fcses, monthly_count_Chicago_pothole_2014_2019_df_1_ts))
print(accuracy(fcnaive, monthly_count_Chicago_pothole_2014_2019_df_1_ts))
print(accuracy(fcholt, monthly_count_Chicago_pothole_2014_2019_df_1_ts))
print(accuracy(fchw, monthly_count_Chicago_pothole_2014_2019_df_1_ts))

# COMMAND ----------

# Plot forecasts
autoplot(fchw)

# COMMAND ----------

fchwa <- hw(train3, seasonal = "additive", h = 12)
autoplot(fchwa)

# COMMAND ----------

# MAGIC %md
# MAGIC Automatic forecasting with exponential smoothing
# MAGIC The namesake function for finding errors, trend, and seasonality (ETS) provides a completely automatic way of producing forecasts for a wide range of time series.

# COMMAND ----------

fiths <- ets(monthly_count_Chicago_pothole_2014_2019_df_1_ts)
autoplot(forecast(fiths))

# COMMAND ----------

checkresiduals(fiths)

# COMMAND ----------

# Fit a seasonal ARIMA model to monthly_count_Chicago_pothole_2014_2019_df_1_ts with lambda = 0
fit <- auto.arima(monthly_count_Chicago_pothole_2014_2019_df_1_ts, lambda = 0)

# COMMAND ----------

# Summarize the fitted model
summary(fit)

# COMMAND ----------

# Plot 2-year forecasts
fit %>% forecast(h = 24) %>% autoplot()

# COMMAND ----------

# Fit a TBATS model to the gas data
fit_tbats <- tbats(monthly_count_Chicago_pothole_2014_2019_df_1_ts)


# COMMAND ----------

# Forecast the series for the next 2 years
fc_tbats <- forecast(fit_tbats, h = 12 * 2)

# COMMAND ----------

# Plot the forecasts
autoplot(fc_tbats)

# COMMAND ----------
#Integrate Azure Machine Learning

