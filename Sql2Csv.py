import sys
from pyspark.sql import *
if __name__ == "__main__":

	database=sys.argv[1]
	table=sys.argv[2]
	username=sys.argv[4]
	passwords=sys.argv[3]
	outputFolder=sys.argv[5]
	
   

	spark = SparkSession.builder \
        .master("local") \
        .appName("Ganit App") \
        .getOrCreate()	
	
	url="jdbc:mysql://localhost:3306/" + database
	
	df=spark.read.format("jdbc").options(url=url,driver="com.mysql.jdbc.Driver",dbtable=table,user=username,password=passwords).load().repartition(5)
	
	df.write.option("header","true").csv(outputFolder)
