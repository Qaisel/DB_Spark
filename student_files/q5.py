import sys
from pyspark.sql import SparkSession
import json
from itertools import combinations

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assignment 2 Question 5").getOrCreate()

df = spark.read.option("header", True).parquet(
    f"hdfs://{hdfs_nn}:9000/assignment2/part2/input/"
)


def process_movie(row):
    movie_id = row.movie_id
    title = row.title

    try:
        cast_data = json.loads(row.cast.replace("'", '"'))
        actors = list(
            {(actor["name"], actor["id"]) for actor in cast_data if "name" in actor}
        )

        actor_pairs = set() 
        for a1, a2 in combinations(actors, 2):
            sorted_pair = tuple(sorted([a1, a2], key=lambda x: x[0])) 
            actor_pairs.add(
                sorted_pair
            ) 
        return [(pair, (movie_id, title)) for pair in actor_pairs]
    except:
        return []


actor_pairs_rdd = df.rdd.flatMap(process_movie)
grouped_pairs = actor_pairs_rdd.distinct().groupByKey().mapValues(list)
filtered_pairs = grouped_pairs.filter(lambda x: len(x[1]) >= 2)
result_rdd = filtered_pairs.flatMap(
    lambda x: [(movie[0], movie[1], x[0][0][0], x[0][1][0]) for movie in x[1]]
)

result_df = spark.createDataFrame(result_rdd, ["movie_id", "title", "actor1", "actor2"])

outputPath = f"hdfs://{hdfs_nn}:9000/assignment2/output/q5.parquet"
result_df.write.mode("overwrite").parquet(outputPath)
