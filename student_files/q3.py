import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, desc
from pyspark.sql.types import FloatType

# you may add more import if you need to
# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW

# Only for loading data
df = spark.read.option("header", True).csv("hdfs://%s:9000/assignment2/part1/input/" % hdfs_nn)

# Cast Rating to Float
df = df.withColumn("Rating", col("Rating").cast(FloatType()))
filtered_df = df.filter((col("City").isNotNull()) & (col("Rating").isNotNull()))
avg_rating_df = filtered_df.groupBy("City").agg(avg("Rating").alias("AverageRating"))
avg_rating_rdd = avg_rating_df.rdd

# Sort descending for Top 2 and ascending for Bottom 2
top2 = avg_rating_rdd.takeOrdered(2, key=lambda x: -x[1])
bottom2 = avg_rating_rdd.takeOrdered(2, key=lambda x: x[1])

# Label the groups
top_group = [(city, rating, "Top") for city, rating in top2]
bottom_group = [(city, rating, "Bottom") for city, rating in bottom2]

# Combine and sort alphabetically by City
final_result = sorted(top_group + bottom_group, key=lambda x: x[0])

# Convert back to DataFrame
result_df = spark.createDataFrame(final_result, ["City", "AverageRating", "RatingGroup"])
final_df = result_df.orderBy(desc("AverageRating"))
# Show result
final_df.show(truncate=False)

final_df.write().csv("Cleaned")
outputPath = f"hdfs://{hdfs_nn}:9000/assignment2/output/question3/"
final_df.write.option("header",True).csv(outputPath)
spark.stop()