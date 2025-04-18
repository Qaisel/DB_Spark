import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assignment 2 Question 4").getOrCreate()

inputPath = f"hdfs://{hdfs_nn}:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv"
df = spark.read.option("header", True).csv(inputPath)

dfFiltered = df.filter((col("City").isNotNull()) & (col("Cuisine Style").isNotNull()))

rdd = dfFiltered.select("City", "Cuisine Style").rdd

def split_cuisines(row):
    city = row["City"]
    raw = row["Cuisine Style"]
    raw = raw.strip("[]")
    items = [x.strip().strip("'") for x in raw.split(",")]
    return [(city, cuisine) for cuisine in items]

flatRdd = rdd.flatMap(split_cuisines)

dfExploded = flatRdd.toDF(["City", "Cuisine"])

resultDf = dfExploded.groupBy("City", "Cuisine").count()

resultDf.show()

outputPath = f"hdfs://{hdfs_nn}:9000/assignment2/output/question4/"
resultDf.write.option("header", True).csv(outputPath)

spark.stop()