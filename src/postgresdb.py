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
#cursor = connection_string.cursor()

#----------------------------------------------------------------------------------------------------------------
#csv_file_path = r'C:\Users\chigb\Downloads\Motor+Vehicle+Thefts+CSV\stolen_vehicles1.csv'
#df = pd.read_csv(csv_file_path)
#print(df)

csv_file_path1 = r'C:\Users\chigb\Downloads\Motor+Vehicle+Thefts+CSV\stolen_vehicles1.csv'

try:
    df1 = pd.read_csv(csv_file_path1)
    print("Read Successful")
except Exception as e:
   print("An error occored: {e}")


#csv_file_path2 = r'C:\Users\chigb\Downloads\Motor+Vehicle+Thefts+CSV\make_details.csv'
#df2 = pd.read_csv(csv_file_path1)
#print(df2)

#csv_file_path3 = r'C:\Users\chigb\Downloads\Motor+Vehicle+Thefts+CSV\locations.csv'
#df3 = pd.read_csv(csv_file_path2)
#print(df3)

#try:
#    df.to_sql('stolen_cars1',con=engine, if_exists= 'replace', index= False)
#    print("Data successfully added to database")
#except Exception as e:
#  print("An error occored: {e}")

try:
    df1.to_sql('stolen_cars3',con=engine, if_exists= 'replace', index= False)
    print("Data successfully added to database")
except Exception as e:
   print("An error occored: {e}")


#try:
#    df2.to_sql('make_details',con=engine, if_exists= 'replace', index= False)
#    print("Data successfully added to database")
#except Exception as e:
#   print("An error occored: {e}")

#try:
#   df3.to_sql('locations',con=engine, if_exists= 'replace', index= False)
#    print("Data successfully added to database")
#except Exception as e:
#   print("An error occored: {e}")
