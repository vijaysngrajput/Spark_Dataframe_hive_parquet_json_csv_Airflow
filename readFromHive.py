#=================================FUNCTIONS  IMPORTED===================================
from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import max, count, avg, grouping, monotonically_increasing_id
#=======================================================================================

#=======================================================================================
sparkHive = SparkSession.builder.enableHiveSupport().getOrCreate()
#sparkHive.sql('create database vijayPractise')
sparkHive.sql('use vijayPractise')

#---------------------------------------------------------------------------------------------------
d1=sparkHive.sql('''select count(gender) AS no_of_females from userdata1 where gender='Female' ''')

d2=sparkHive.sql('''select count(gender) AS no_of_males from userdata1 where gender='Male' ''')
#---------------------------------------------------------------------------------------------------
d3=sparkHive.sql('''select avg(salary) AS avg_of_females from userdata1 where gender='Female' ''')

d4=sparkHive.sql('''select avg(salary) AS avg_of_males from userdata1 where gender='Male' ''')

d5=sparkHive.sql('''select avg(salary) AS avg_of_org from userdata1 ''')
#---------------------------------------------------------------------------------------------------
d6=sparkHive.sql('''select max(salary) AS max_of_females from userdata1 where gender='Female' ''')

d7=sparkHive.sql('''select max(salary) AS max_of_males from userdata1 where gender='Male' ''')

d8=sparkHive.sql('''select max(salary) AS max_of_org from userdata1 ''')
#---------------------------------------------------------------------------------------------------

#=====================MERGING DATAFRAMES IN SINGLE TABLE============================================
d1 = d1.withColumn("id", monotonically_increasing_id())

d2 = d2.withColumn("id", monotonically_increasing_id())

d3 = d3.withColumn("id", monotonically_increasing_id())

d4 = d4.withColumn("id", monotonically_increasing_id())

d5 = d5.withColumn("id", monotonically_increasing_id())

d6 = d6.withColumn("id", monotonically_increasing_id())

d7 = d7.withColumn("id", monotonically_increasing_id())

d8 = d8.withColumn("id", monotonically_increasing_id())


x1 = d2.join(d1,"id", "outer").drop("id")
x2 = d4.join(d3,"id", "outer").drop("id")
x3 = d6.join(d5,"id", "outer").drop("id")
x4 = d8.join(d7,"id", "outer").drop("id")



x1 = x1.withColumn("id", monotonically_increasing_id())
x2 = x2.withColumn("id", monotonically_increasing_id())
x3 = x3.withColumn("id", monotonically_increasing_id())
x4 = x4.withColumn("id", monotonically_increasing_id())

y1 = x2.join(x1,"id", "outer").drop("id")
y2 = x4.join(x3,"id", "outer").drop("id")

y1 = y1.withColumn("id", monotonically_increasing_id())
y2 = y2.withColumn("id", monotonically_increasing_id())

z1 = y2.join(y1,"id", "outer").drop("id")
#z1.show()
#================================================================================================
z1.createOrReplaceTempView("table1")
ob22 = sparkHive.sql("SELECT * from table1")

#ob22.show()

#dataframe to json

ob22.select("*").write.save("file:///home/mapr/vijayPractise/final.json",format="json")

#convert json to parquet

nn = sparkHive.read.json("file:///home/mapr/vijayPractise/final.json")

nn.select("*").write.save("file:///home/mapr/vijayPractise/final.parquet",format="parquet")


print "=====================================task completed======================================="
#===================================================================================================
