
Youtube Videos Data Loader
Description:

This tool is designed to load YouTube video data into a MS SQL database. It supports SCD 2 Type and allows you to configure the number of videos to load per run.

Technologies:

Python
SQL
Apache Airflow
Features:

Loads YouTube video information from the YouTube API
Transforms data into SCD 2 Type format
Loads data into a MS SQL database
Configures the number of videos to load per run
Benefits:

Automated data loading
SCD 2 Type support
Scalability
Configurability
How to use:

Install Python and the required libraries.
Run the tool.
Configuration variables to start (in Aiflow) example:

ms_sql_conn_string: mssql+pyodbc://admin:admin@localhost/Youtube_analytic?&driver=ODBC Driver 17 for SQL Server
yt_apikey: ххх

Links:

YouTube API: https://developers.google.com/youtube/v3/
SCD 2 Type: https://en.wikipedia.org/wiki/Slowly_changing_dimension
Apache Airflow: https://airflow.apache.org/
