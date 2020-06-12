---
page_type: Official Microsoft Sample
languages:
- R, SparkR, SparkSQL
products:
- Azure Databricks | Azure Blob Storage | Azure OpenDatasets | Azure KeyVault
description: "Analyzing the safety (311) dataset published by Azure Open Datasets for Chicago, Boston and New York City using SparkR, SparkSQL, Azure Databricks, visualization using ggplot2 and leaflet. Focus is on descriptive analytics, visualization, clustering, time series forecasting and anomaly detection."
urlFragment: "https://github.com/microsoft/A-TALE-OF-THREE-CITIES"
---

# A TALE OF THREE CITIES

<!-- 
Guidelines on README format: https://review.docs.microsoft.com/help/onboard/admin/samples/concepts/readme-template?branch=master

Guidance on onboarding samples to docs.microsoft.com/samples: https://review.docs.microsoft.com/help/onboard/admin/samples/process/onboarding?branch=master

Taxonomies for products and languages: https://review.docs.microsoft.com/new-hope/information-architecture/metadata/taxonomies?branch=master
-->
![intro_bike](images/intro_bike.jpg)
## Introduction

  With 174 hours of biking and a bit of ferry combined you can start from Boston, touch Chicago, and reach the city of New York.  However different these cities are, they all have one thing in common the 311 service. 
  
  The telephone number 3-1-1 creates a central hub for local subscribers to access a variety of city services. 311 provides access to non-emergency municipal services from sewer concerns, pothole problems, abandoned car removal and neighborhood complaints to graffiti removal. This service is available to divert routine inquiries and non-urgent community concerns from the 9-1-1 number which is reserved for emergency service. A promotional website for 3-1-1 in Akron described the distinction as follows: “Burning building? Call 9-1-1. Burning question? Call 3-1-1” (wiki/3-1-1, n.d.) 
  
  A recent 15-city study of 311 by the Pew Charitable Trusts found that the average cost per 311 call is $3.39. Detroit came in with the highest cost per call at a whopping $7.78. Despite the excessive costs, cities do not appear to be slowing their migration to 311. In fact, many are pushing forward with faith that the increased efficiency, streamlined processes and customer satisfaction they achieve will ultimately pay off (Brown, 2012) .
  
  We have identified the 3-1-1 call dataset from the cities Chicago, Boston and New York city provided by Azure Open Datasets. We believe that data is the new currency, now the question becomes what can we do with the 3-1-1 data and how can that analysis be beneficial?
  
## Value Proposition

  This analysis can serve as an exploratory reference, and with refinement can be reused in the optimization of the Maintenance Fiscal budget of a city. The Development and the Maintenance services budget includes General Services, Public Works, Planning & Development and Solid Waste Management. This budget occupies a large portion in a city's overall fiscal budget and by the application and refinement of the descriptive and predictive analytics demonstrated as part of this work we can statistically optimize and predict the overall spending and budgeting. Here is an example of City of Houston’s 2019 Fiscal Year budget breakdown to give an idea of the general breakdown of the development and maintenance services components: https://www.houstontx.gov/budget/19budadopt/I_TABI.pdf
  
Secondly, this work can be used as a workshop, reference material and self-learning for the following concepts, technologies, and platforms:
* Data Engineering using SparkR, R ecosystem
* Data visualization and descriptive analytics
* Time Series forecasting
* Anomaly detection
* Products used: Azure Databricks, Azure Open Datasets, Azure Blob Storage

In the second phase I plan to develop another flavor solution using Azure Synapse Analytics and Azure Machine learning primarily using Python, REST APIs and PySpark.

## Focus Area

  For the purpose of this analysis, I want to examine how the incidents reported in these three cities are related albeit imperfectly with time, clusters of incidents. Some of the questions and problems that is addressed are as follows:
  * Transformation and enrichment of the datasets.
  * Perform descriptive analytics on the data.
  * Time series analysis and visualization
  * Cluster visualization and analysis
  * Time series forecasting and comparison using various methods
  * Anomaly detection and reporting 
  * Correlation among the incidents occurring the three cities w.r.t time
  
  Because of the varied nature of the incidents and analysis (descriptive and predictive) that can be performed on them, I demonstrated some of the concepts by means of isolating the pothole repair complaints which also ranks within the top 10 categories of complaints in the three cities (as we will demonstrate here as well). However, these methodologies can be seamlessly applied and reused across other categories of complaints with little modification.

## Guiding Principles
The work that will be subsequently done as part of this paper will have at the very least embody the following principles (ai/responsible-ai, n.d.):
*	Fair - AI must maximize efficiencies without destroying dignity and guard against bias
*	Accountable - AI must have algorithmic accountability
*	Transparent - AI systems must be transparent and understandable
*	Ethical - AI must assist humanity and be designed for intelligent privacy

## Contents

| File/folder       | Description                                |
|-------------------|--------------------------------------------|
| `code`            | Sample source code.                        |
| `dbc`             | Azure Databricks dbc files.                |
| `images`          | Sample images used for documentation.      |
| `.gitignore`      | Define what to ignore at commit time.      |
| `CHANGELOG.md`    | List of changes to the sample.             |
| `CONTRIBUTING.md` | Guidelines for contributing to the sample. |
| `README.md`       | This README file.                          |
| `LICENSE`         | The license for the sample.                |

## Target Audience
* Data Scientists
* Data Engineers
* Architects
* R and Spark Developers

## Pre-Requisite Knowledge
* Prior knowledge of Spark, is beneficial
* Familiarity/experience with R and Azure

## Azure Pre-Requisites
A subscription with at least $200 credit for a continuous 15-20 hours of usage.

## Building Blocks
The building blocks section of constitutes of the source dataset, technologies and platform used.
Refer [Building Blocks](https://github.com/microsoft/A-TALE-OF-THREE-CITIES/wiki/Building-Blocks) from the project wiki for detailed description.

## Architecture of the solution 
Refer [Architecture and Process Flow](https://github.com/microsoft/A-TALE-OF-THREE-CITIES/wiki/Architecture-of-the-solution-and-Process-flow) from the project wiki for the architecture of the solution and the process flow.

## Data Wrangling, Exploration and Visualization
Data is cleansed and enriched using SparkR and SparkSQL. The curated dataset is written in Azure Blob storage in parquet format (parquet.apache.org, n.d.) partitioned by City Name. Data exploration and visualization is done using SparkR, SparkSQL, ggplot2, htmltools, htmlwidgets, leaflet with ESRI plugin, magrittr etc.
Refer [Data Wrangling, Exploration and Visualization](https://github.com/microsoft/A-TALE-OF-THREE-CITIES/wiki/Data-Wrangling,-Exploration-and-Visualization) from the project wiki for details.

## Problem Isolation
Because of the varied nature of the incidents we tried to demonstrate the concepts using the pothole complaints. Pothole facts from wiki (Pothole#Costs_to_the_public, n.d.) The American Automobile Association estimated in the five years prior to 2016 that 16 million drivers in the United States have suffered damage from potholes to their vehicle including tire punctures, bent wheels, and damaged suspensions with a cost of $3 billion a year. In India, 3,000 people per year are killed in accidents involving potholes. Britain has estimated that the cost of fixing all roads with potholes in the country would cost £12 billion. As mentioned earlier, these methodologies can be seamlessly applied and reused across other categories of complaints with little modification.

## Time Series Analysis and Forecasting
The time series analysis and forecasting are done through SparkR, ggplot2, forecast, ggfortify etc. A time series can be thought of as a vector or matrix of numbers along with some information about what times those numbers were recorded. This information is stored in a ts object in R. ts(data, start, frequency, ...) 
Refer [Time Series Analysis and Forecasting](https://github.com/microsoft/A-TALE-OF-THREE-CITIES/wiki/Overview-of-Time-Series-Analysis-and-Forecasting-and-Anomaly-Detection#time-series-analysis-and-forecasting) from the project wiki for details.

## Anomaly Detection
The time series anomaly detection is done through SparkR, ggplot2, tidyverse and anomalize (anomalize, n.d.) package. By using anomalize package we have decomposed time series, detected anomalies, and created bands separating the “normal” data from the anomalous data. Refer [Anamoly Detection](https://github.com/microsoft/A-TALE-OF-THREE-CITIES/wiki/Overview-of-Time-Series-Analysis-and-Forecasting-and-Anomaly-Detection#anomaly-detection) from the project wiki for details.

## Setup and Running the code
Refer [Setup and Running the code](https://github.com/microsoft/A-TALE-OF-THREE-CITIES/wiki/Setup-and-Running-the-code) from the project wiki for step by step instructions on how to setup and run the code.

## References
* (n.d.). Retrieved from parquet.apache.org: https://parquet.apache.org/
* ai/responsible-ai. (n.d.). Retrieved from microsoft.com: https://www.microsoft.com/en-us/ai/responsible-ai
* anomalize. (n.d.). Retrieved from github.com: https://github.com/business-science/anomalize
* azure.microsoft.com. (n.d.). Retrieved from https://azure.microsoft.com/en-us/services/databricks/
* Brown, J. (2012, May 31). budget-finance. Retrieved from govtech.com: https://www.govtech.com/budget-finance/Cities-Aim-to-Slash-311-Phone-Bills-Without-Affecting-311-Services.html
* data.boston.gov. (n.d.). Retrieved from https://data.boston.gov/dataset/311-service-requests
* data.cityofchicago.org. (n.d.). Retrieved from https://data.cityofchicago.org/Service-Requests/311-Service-Requests-Sanitation-Code-Complaints-Hi/me59-5fac
* data.cityofnewyork.us. (n.d.). Retrieved from https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9
* docs.microsoft.com. (n.d.). Retrieved from https://docs.microsoft.com/en-us/azure/open-datasets/overview-what-are-open-datasets
* Pothole#Costs_to_the_public. (n.d.). Retrieved from en.wikipedia.org: https://en.wikipedia.org/wiki/Pothole#Costs_to_the_public
* spark.apache.org. (n.d.). Retrieved from https://spark.apache.org/docs/latest/sparkr.html
* spark.apache.org. (n.d.). Retrieved from https://spark.apache.org/docs/latest/sql-programming-guide.html
* storage/blobs/. (n.d.). Retrieved from microsoft.com: https://azure.microsoft.com/en-us/services/storage/blobs/
* wiki/3-1-1. (n.d.). Retrieved from en.wikipedia.org: https://en.wikipedia.org/wiki/3-1-1


## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
