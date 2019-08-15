from pyspark.sql import SparkSession

# Create the spark session
spark = SparkSession.builder.getOrCreate()

data_path = "./Data"

filepath = data_path + "/location_temp.csv"

# Read JSON
df1 = spark.read.format("csv").option("header", "true").load(filepath)
df1.count()
df1.show()

# What if we want to see a specific location?
# Allows us to basically specify a where 
df1.filter(df1["location_id"] == "loc0").count()
df1.filter(df1["location_id"] == "loc0").show()

df1.filter(df1["location_id"] == "loc1").count()
df1.filter(df1["location_id"] == "loc1").show()

