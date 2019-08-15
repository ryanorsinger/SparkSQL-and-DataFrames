from pyspark.sql import SparkSession

# Create the spark session
spark = SparkSession.builder.getOrCreate()

data_path = "./Data"

json_df2_path = data_path + "/utilization.json"

# Read JSON
df2 = spark.read.format("json").load(json_df1_path)

# Basic outlines
df2.show(10)
df2.count()

# Show column names (attribute of df, not a method or function)
df2.columns

# Make a sample (with or without replacement), False means no replacement, second argument is a fraction
df2_sample = df2.sample(False, fraction=0.1) #sample 10% of the data

df2_sample.show()

# Sorting a dataframe by a specified column
df2_sort = df2.sort("event_datetime")

df2_sort.show()