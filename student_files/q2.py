import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, asc
# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW
inputPath = f"hdfs://{hdfs_nn}:9000/assignment2/part1/input/"
df = spark.read.option("header",True).csv(inputPath)

dfWithPriceRange = df.filter((col("Price Range").isNotNull()) & (col("Rating").isNotNull()))

dfConvertRating =  dfWithPriceRange.withColumn("Rating", col("Rating").cast("float"))

combinations = dfConvertRating.select(col("City"),col("Price Range")).distinct().collect()

results = []

for combi in combinations:
    city = combi["City"]
    priceRange = combi["Price Range"]
    current = dfConvertRating.filter((col("City") == city) & (col("Price Range") == priceRange))
    best = current.orderBy(desc(col("Rating"))).first()
    worst = current.orderBy(asc(col("Rating"))).first()
    results.append((
            city,
            priceRange,
            best["Name"],
            float(best["Rating"]),
            worst["Name"],
            float(worst["Rating"])
        ))
    
resultDf = spark.createDataFrame(results, ["City","Price Range", "Best Restaurant","Best Rating","Worst Restaurant","Worst Rating"])

outputPath = f"hdfs://{hdfs_nn}:9000/assignment2/output/question2/"
resultDf.write.option("header",True).csv(outputPath)

spark.stop()