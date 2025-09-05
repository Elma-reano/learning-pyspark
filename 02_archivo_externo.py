
from pyspark.sql import SparkSession

spark = SparkSession.builder \
            .appName("TestApp1") \
            .getOrCreate()

spark_context = spark.sparkContext
file_path = 'profesionistas.csv'

rdd = spark_context.textFile(file_path)

if rdd.isEmpty():
    print("El archivo está vacío o no se pudo leer.")
else:
    header = rdd.first()
    data = rdd.filter(lambda row: row != header)
    data = data.map(lambda line: line.split(","))

    print("Primeros Registros")
    for registro in data.take(5):
        print(registro)

spark.stop()

