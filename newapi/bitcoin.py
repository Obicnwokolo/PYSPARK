import pandas as pd
import requests
import sqlalchemy as sa
import pandas as pd

# importing connection engine pack
from sqlalchemy import create_engine, inspect, text
from urllib.parse import quote_plus #why
from sqlalchemy import Table, MetaData

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
print("TimeSeries_data:")
print(df_TimeSeries_data)

df_TimeSeries_data.to_csv("time_series_data.csv", index=False)


try:
    meta_data.to_sql('bitcoin_meta_data',con=engine, if_exists= 'replace', index= False)
    print("Data successfully added to database")
except Exception as e:
   print("An error occored: {e}")


try:
    TimeSeries_data.to_sql('bitcoin_TimeSeries_data',con=engine, if_exists= 'replace', index= False)
    print("Data successfully added to database")
except Exception as e:
   print("An error occored: {e}")

