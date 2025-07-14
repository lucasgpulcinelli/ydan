# YDAn (Youtube Data Analysis, main project)
YDAn (pronounced like "you done") is a data engineering project about gathering youtube video and channel data recursively and then processing it to obtain meaningful information in visual dashboards using analytical processing techniques such a fact constellation schema and data warehousing principles.

## Structure of the project
This project is made in several parts:
- postgres\_init/ and docker-compose.yml, containing the necessary code to deploy the web scraper, with a postgres database and a minio object store,
- scraper/, containing the web scraper developed with clojure to get youtube data from a seed channel recursively via recommendations,
- insertion/, with pyspark code to populate tables in a snowflake data warehouse with data from minio (we needed to export data from the scraper using json previously because of time-related constraints in the other ydan related projects)
- dbt/models/, with SQL code to transform all raw scraping snowflake tables to a dimensional model
- dbt/seed\_tables/, with CSV files used as extra static data using during the transformation process
- queries/, with SQL queries to create dashboards with snowflake with our results.

## Made Fully By
- Lucas Eduardo Gulka Pulcinelli,
- Matheus Pereira Dias,
- Vinicio Yusuke Hayashibara,
- .

