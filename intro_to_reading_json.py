from pyspark.sql import SparkSession

# Create the spark session
spark = SparkSession.builder.getOrCreate()

data_path = "./Data"

json_df1_path = data_path + "/utilization.json"

# Read JSON
df1 = spark.read.format("json").load(json_df1_path)
df1.count()
df1.show()

