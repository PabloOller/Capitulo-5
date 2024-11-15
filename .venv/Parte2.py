# In Python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Parte 2").getOrCreate()


schema = StructType([StructField("celsius", ArrayType(IntegerType()))])
t_list = [[35, 36, 32, 30, 40, 42, 38]], [[31, 32, 34, 55, 56]]
t_c = spark.createDataFrame(t_list, schema)
t_c.createOrReplaceTempView("tC")
# Show the DataFrame
t_c.show(truncate = False)

spark.sql("""
SELECT celsius,  transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit
 FROM tC
""").show(truncate = False)

spark.sql("""
SELECT celsius,
 filter(celsius, t -> t > 38) as high
 FROM tC
""").show(truncate = False)

spark.sql("""
SELECT celsius,
 exists(celsius, t -> t = 38) as threshold
 FROM tC
""").show(truncate = False)

# Calcular la media en Fahrenheit directamente en SQL
# Calcular la media en Fahrenheit directamente en SQL
spark.sql("""
SELECT celsius,
 aggregate(
 celsius,
 0,
 (t, acc) -> t + acc,
 acc -> (acc div size(celsius) * 9 / 5) + 32
 ) as avgFahrenheit
 FROM tC
""").show()



spark.stop()