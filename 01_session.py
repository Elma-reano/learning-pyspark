
from pyspark.sql import SparkSession

spark = SparkSession.builder \
            .appName("TestApp1") \
            .getOrCreate()