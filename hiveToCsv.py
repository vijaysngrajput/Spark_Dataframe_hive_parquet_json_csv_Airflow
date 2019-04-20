from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import *

sparkHive = SparkSession.builder.enableHiveSupport().getOrCreate()

sparkHive.sql('use vijayPractise')

d=sparkHive.sql('select * from userdata1')

d.write.csv("file:///home/mapr/vijayPractise/final.csv",header=True)

