from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("MiniProj").enableHiveSupport().getOrCreate()
max_id = spark.sql("SELECT max(id) FROM product.emp_info")
m_id = max_id.collect()[0][0]
str(m_id)

query = 'SELECT * FROM emp_info WHERE "ID" > ' + str(m_id)

more_data = df = spark.read.format("jdbc").option("url", "jdbc:postgresql://18.132.73.146:5432/testdb").option("driver", "org.postgresql.Driver").option("dbtable", "stolen_vehicles").option("user", "consultants").option("password", "WelcomeItc@2022").option("query", query).load()


df_age = more_data.withColumn("DOB", to_date(col("DOB"), "M/d/yyyy")) \
    .withColumn("age", floor(datediff(current_date(), col("DOB")) / 365))
df_age.show(10)

# Define the increments based on departments and gender
department_increment_expr = when(col("dept") == "IT", 0.1) \
    .when(col("dept") == "Marketing", 0.12) \
    .when(col("dept") == "Purchasing", 0.15) \
    .when(col("dept") == "Operations", 0.18) \
    .when(col("dept") == "Finance", 0.2) \
    .when(col("dept") == "Management", 0.25) \
    .when(col("dept") == "Research and Development", 0.15) \
    .when(col("dept") == "Sales", 0.18) \
    .when(col("dept") == "Accounting", 0.15) \
    .when(col("dept") == "Human Resources", 0.12) \
    .otherwise(0)

# Calculate the increment based on department and gender
increment_expr = when(col("gender") == "Female", department_increment_expr + 0.1).otherwise(department_increment_expr)

# Calculate the incremented salary based on department and gender
df_increment = df_age.withColumn("increment", col("salary") * increment_expr) \
    .withColumn("new_salary", col("salary") + col("increment"))

# Show the updated DataFrame
df_increment.show(10)

# Sort the DataFrame by ID
sorted_df = df_increment.orderBy("ID")
sorted_df.show(10)

df_increment.write.mode("append").saveAsTable("product.emp_info")
print("Successfully Load to Hive")

# spark-submit --master local[*] --jars /var/lib/jenkins/workspace/nagaranipysparkdryrun/lib/postgresql-42.5.3.jar src/IncreamentalLoadPostgressToHive.py

# df2 = spark.read.csv("path/to/other_file.csv", header=True, inferSchema=True)
# joined_df = df.join(df2, on=["ID"], how="inner")

# df1.write.mode("overwrite").saveAsTable("product.dummy")
# hadoop fs -chmod -R 775 /warehouse/tablespace/external/hive/product.db/emp_info
# sudo -u hdfs hdfs dfs -chmod -R 777 /warehouse/tablespace/external/hive/product.db