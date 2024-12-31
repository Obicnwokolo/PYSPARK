from pyspark.sql import *
from pyspark.sql.functions import *

# create spark session
spark = SparkSession.builder.master("local").appName("read db").config("spark.jars", "/path/to/postgresql.jar").getOrCreate()

# PostgreSQL configurations
pg_url = "jdbc:postgresql://18.132.73.146:5432/testdb"
pg_properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver"
}

# Step 1: Load the maximum ID from PostgreSQL
max_index_query = "(SELECT MAX(index) AS max_id FROM sop_stock_timeSeries_data) AS max_id_table"
max_index_df = spark.read.jdbc(url=pg_url, table=max_index_query, properties=pg_properties)

# Get the maximum ID value
max_index = max_index_df.collect()[0]["max_id"]
print("Max ID in the PostgreSQL table: {max_id}")

# Step 2: Load records with IDs greater than the max ID
incremental_query = f"(SELECT * FROM sop_stock_timeSeries_data WHERE index > {max_id}) AS incremental_table"
incremental_df = spark.read.jdbc(url=pg_url, table=incremental_query, properties=pg_properties)

# Step 3: Process or save the incremental data
incremental_df.show()  # Display the records
#------------------------------------------------------------------------------------------------
# define input sources
# Creating connection strings for my database
username= "consultants"
password = "WelcomeItc@2022"
host= "18.132.73.146"
port = "5432"
database= "testdb"
#ENCODED_PASSWORD = quote_plus(password)


#creating database connectionw string
connection_string = engine = create_engine('postgresql://consultants:WelcomeItc%402022@18.132.73.146:5432/testdb')
# Establishing connection with engine & database
try:
    engine = create_engine(connection_string)
    print("Connection Successful")
except Exception as e:
   print("An error occored: {e}")
#cursor = connection_string.cursor()
#----------------------------------------------------------------------------------------------
import pandas as pd
import requests
import sqlalchemy as sa

# importing connection engine pack
from sqlalchemy import create_engine, inspect, text
from urllib.parse import quote_plus #why
from sqlalchemy import Table, MetaData

#-------------------------------------------------------------------------------------------

url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=T9VJ3JP8JPENROGG'
header ={"Content-Type":"application/json",
         "Accept-Encoding":"deflate"}
response = requests.get(url,headers=header)
print(response)
responseData = response.json()
print(responseData)

meta_data= responseData.get("Meta Data")
df_meta_data= pd.DataFrame([meta_data])
print("Meta Data:")
print(df_meta_data)

TimeSeries_data= responseData.get("Time Series (5min)")
df_TimeSeries_data= pd.DataFrame.from_dict(TimeSeries_data, orient="index").reset_index()
incrdf_TimeSeries_data= df_TimeSeries_data.filter(df_TimeSeries_data.id > max_index)
print("TimeSeries_data:")
print(df_TimeSeries_data)

try:
    df_meta_data.to_sql('sop_stock_meta_data',con=engine, if_exists= 'replace', index= False)
    print("Data successfully added to database")
except Exception as e:
   print("An error occored: {e}")


try:
    df_TimeSeries_data.to_sql('sop_stock_timeSeries_data',con=engine, if_exists= 'append', index= False)
    print("Data successfully added to database")
except Exception as e:
   print("An error occored: {e}")

