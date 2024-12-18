from pyspark.sql import *
from pyspark.sql.functions import *

# Initialize spark session
spark = SparkSession.builder \
    .appName("bank_churn") \
    .master("local[*]") \
    .getOrCreate()

# Read the Csv file
df1= spark.read.option("header", "true").csv("C:/Users/chigb/Downloads/Bank_Churn.csv")
df1.show(3)

#Filtered the data based on Gender(Male)
filtered_df1 = df1.filter(df1["Gender"]== "Male")
filtered_df1.show(5)

# Added 2 New columns
from pyspark.sql.functions import lit, length, when
df1 = df1.withColumn("LenName", length(df1["Surname"]))
df1 = df1.withColumn("LossType", when(df1["CreditScore"] >= 700, "Big loss").otherwise("loss"))
df1.show(5)

# Dropped LenName column
df1= df1.drop("LenName")
df1.show(5)

# Changed Column name from Geography to Location
df1 = df1.withColumnRenamed("Geography", "Location")

# Group By NumOfProducts
group_df= df1.groupBy("NumOfProducts").count()
group_df.show(5)

# Sort Table by CustomerId
df1_sorted = df1.orderBy("CustomerId")
df1_sorted.show()

# Replace Values of Active members
mod_df1 =df1.withColumn("IsActiveMember", when(df1["IsActiveMember"]== 1, "Yes").otherwise("No"))
mod_df1.show(5)

# Change CustomerId from String data type to integer
df1= df1.withColumn("CustomerId", df1["CustomerId"].cast("int"))
df1.printSchema()

# Normalize Expected Salary
CS_mean= df1.agg(mean("EstimatedSalary")).collect()[0][0]
CS_stddev= df1.agg(stddev("EstimatedSalary")).collect()[0][0]
df1 = df1.withColumn("Standardaized Est_Salary", (col("EstimatedSalary") - CS_mean) / CS_stddev)
df1.show(5)

Bank_churn = df1

Bank_churn.show(5)

#Bank_churn.write.option("header", "true").csv("C:/Users/chigb/Downloads/Mod_Bank_churn")