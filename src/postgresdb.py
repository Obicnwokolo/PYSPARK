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
ENCODED_PASSWORD = quote_plus(password)


#creating database connectionw string
connection_string = f"postgresql+psycopg2://{username}:{ENCODED_PASSWORD}@{host}:{port}/{database}"

# Establishing connection with engine & database
engine = create_engine(connection_string)
#cursor = connection_string.cursor()

#----------------------------------------------------------------------------------------------------------------
csv_file_path1 = r'C:\Users\chigb\Downloads\Motor+Vehicle+Thefts+CSV\stolen_vehicles2.csv'
df1 = pd.read_csv(csv_file_path1)
print(df1)

csv_file_path2 = r'C:\Users\chigb\Downloads\Motor+Vehicle+Thefts+CSV\make_details.csv'
df2 = pd.read_csv(csv_file_path1)
print(df2)

csv_file_path3 = r'C:\Users\chigb\Downloads\Motor+Vehicle+Thefts+CSV\locations.csv'
df3 = pd.read_csv(csv_file_path2)
print(df3)

try:
    df1.to_sql('stolen_vehicles',con=engine, if_exists= 'replace', index= False)
    print("Data successfully added to database")
except Exception as e:
   print("An error occored: {e}")


try:
    df2.to_sql('make_details',con=engine, if_exists= 'replace', index= False)
    print("Data successfully added to database")
except Exception as e:
   print("An error occored: {e}")

try:
    df3.to_sql('locations',con=engine, if_exists= 'replace', index= False)
    print("Data successfully added to database")
except Exception as e:
   print("An error occored: {e}")
