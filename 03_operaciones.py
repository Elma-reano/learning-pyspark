
from pyspark.sql import SparkSession

spark = SparkSession.builder \
            .appName("TestApp1") \
            .getOrCreate()

spark_context = spark.sparkContext

## Map
map_data = [i+1 for i in range(10)]
rdd_to_map = spark_context.parallelize(map_data)

rdd_squared = rdd_to_map.map(lambda x: x**2)
print("Números al cuadrado:", rdd_squared.collect())

print("-------------------------------")

## Filter
filter_data = [45, 28, 50, 25, 32, 18, 29, 35, 22, 40]
rdd_to_filter = spark_context.parallelize(filter_data)

rdd_filtered = rdd_to_filter.filter(lambda x: x > 30)
print("Números mayores a 30:", rdd_filtered.collect())

print("-------------------------------")

## FlatMap

flatmap_data = [
    "En un lugar de la Mancha de cuyo nombre no quiero acordarme",
    "Hace mucho tiempo, en una galaxia muy muy lejana",
    "Era el mejor de los tiempos, era el peor de los tiempos",
]
rdd_to_flatmap = spark_context.parallelize(flatmap_data)

rdd_words = rdd_to_flatmap.flatMap(lambda line: line.split(" "))
print("Palabras individuales:", rdd_words.collect())

print("-------------------------------")

## Union

rdd1 = spark_context.parallelize([1, 2, 3, 6])
rdd2 = spark_context.parallelize([4, 5, 6])
# Note que el 6 se repite en ambos RDDs, y no se va a eliminar

union_rdd = rdd1.union(rdd2)
print("Unión de RDDs:", union_rdd.collect())

print("-------------------------------")

## ReduceByKey
pair_data = [
    ("cliente1", 100),
    ("cliente2", 200),
    ("cliente1", 150),
    ("cliente3", 100),
    ("cliente2", 150),
    ("cliente3", 200)
    ]
pair_rdd = spark_context.parallelize(pair_data)

total_by_client = pair_rdd.reduceByKey(lambda a, b: a + b)
print("Total por cliente")
for client, total in total_by_client.collect():
    print(f"{client}: {total}")

print("-------------------------------")

spark.stop()