import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import spark as spark
# Create cubed function

spark = SparkSession.builder.appName("Parte 1").getOrCreate()



def cubed(s):
 if s is None:
  return None
 return s * s * s

# Register UDF
spark.udf.register("cubed", cubed, LongType())
# Generate temporary view
spark.range(1, 9).createOrReplaceTempView("udf_test")

spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()

# Declare the cubed function
def cubed2(a: pd.Series) -> pd.Series:
 return a * a * a
# Create the pandas UDF for the cubed function
cubed_udf = pandas_udf(cubed2, returnType=LongType())

x = pd.Series([1, 2, 3])
# The function for a pandas_udf executed with local Pandas data
print(cubed2(x))

'''
# Create a Spark DataFrame, 'spark' is an existing SparkSession
df = spark.range(1, 4)
# Execute function as a Spark vectorized UDF
df.select("id", cubed_udf(col("id"))).show()
'''

spark.stop()