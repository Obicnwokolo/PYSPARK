from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("MiniProj").enableHiveSupport().getOrCreate()

max_vehicle_id = spark.sql("SELECT max(vehicle_id) FROM bigdata_nov_2024.stolen_vehicles")
m_vehicle_id = max_vehicle_id.collect()[0][0]

query = 'SELECT * FROM stolen_cars WHERE vehicle_id > ' + str(m_vehicle_id)

more_data = spark.read.format("jdbc").option("url", "jdbc:postgresql://18.132.73.146:5432/testdb").option("driver", "org.postgresql.Driver").option("user", "consultants").option("password", "WelcomeItc@2022").option("query", query).load()

more_data.write.mode("append").saveAsTable("bigdata_nov_2024.stolen_vehicles")
print("Successfully Load to Hive")

# spark-submit --master local[*] --jars /var/lib/jenkins/workspace/nagaranipysparkdryrun/lib/postgresql-42.5.3.jar src/IncreamentalLoadPostgressToHive.py

# df2 = spark.read.csv("path/to/other_file.csv", header=True, inferSchema=True)
# joined_df = df.join(df2, on=["ID"], how="inner")

# df1.write.mode("overwrite").saveAsTable("product.dummy")
# hadoop fs -chmod -R 775 /warehouse/tablespace/external/hive/product.db/emp_info
# sudo -u hdfs hdfs dfs -chmod -R 777 /warehouse/tablespace/external/hive/product.db