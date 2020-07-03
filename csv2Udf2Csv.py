import sys
from pyspark.sql import *
from pyspark.sql.functions import udf, col
from pyspark.sql.types import FloatType
import math

def sqrtAmount(columnName):
     return math.sqrt(columnName)


if __name__ == "__main__":

	inputPath=sys.argv[1]
	outputPath=sys.argv[2]

	spark = SparkSession.builder \
        .master("local") \
        .appName("Ganit App") \
        .getOrCreate()	
	
	sqrtUdf = udf(lambda columnName: sqrtAmount(columnName))


	
	df=spark.read.option("header","true").csv(inputPath).withColumn("tran_amount", col("tran_amount").cast(FloatType()))
	
	df.printSchema()
	

	df.withColumn("NewValue",sqrtUdf(col("tran_amount"))).write.option("header","true").csv(outputPath)
