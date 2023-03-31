import os
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession,SQLContext
from pyspark import SparkContext as sc

import findspark
findspark.init("C:\\Users\\abhun\\Downloads\\spark-3.3.1-bin-hadoop3")

spark = SparkSession.builder.master("local[*]").getOrCreate()
sc = pyspark.SparkContext.getOrCreate()
sqlcontext = SQLContext(sc)

file = spark.sparkContext.textFile("C://cls//empdata.log")
filterfile = file.filter( lambda x : "Bangalore" in x)
print(filterfile.count())

for x in filterfile.collect():print(x)