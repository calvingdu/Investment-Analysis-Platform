# Import the necessary modules
from __future__ import annotations

from pyspark.sql import SparkSession

# Create a SparkSession


def spark_job():
    spark = SparkSession.builder.appName("My App").getOrCreate()

    rdd = spark.sparkContext.parallelize(range(1, 100))

    print("THE SUM IS HERE: ", rdd.sum())
    # Stop the SparkSession
    spark.stop()
    return rdd.sum()
