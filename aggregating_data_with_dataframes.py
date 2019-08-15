from pyspark.sql import SparkSession

# Create the spark session
spark = SparkSession.builder.getOrCreate()

data_path = "./Data"

filepath = data_path + "/location_temp.csv"

# Read JSON
df1 = spark.read.format("csv").option("header", "true").load(filepath)
df1.count()
df1.show(10)

# Count how many measurements on each location
df1.groupby("location_id").count().show()

# Order by location_id
df1.orderBy("location_id").show()


# Get average temp_celcius grouped by location_id
df1.groupBy("location_id").agg({"temp_celcius": "mean"}).show()

# Get the minimu, temp_celcius recorded at each location_id
df1.groupBy("location_id").agg({"temp_celcius": "max"}).show()

# Get the max temperature recorded at each location
df1.groupBy("location_id").agg({"temp_celcius": "min"}).show()
