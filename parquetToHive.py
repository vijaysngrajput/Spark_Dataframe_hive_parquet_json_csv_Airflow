from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import *

sparkHive = SparkSession.builder.enableHiveSupport().getOrCreate()

sparkHive.sql('use vijayPractise')

sparkHive.sql('drop table userdata1')

sparkHive.sql('create table userdata1 \
         (registration_dttm TIMESTAMP,id int,first_name string,last_name string,email string,gender string,ip_address string,cc string,country string,birthdate string,salary double,title string,comments string) \
         stored as parquet')


sparkHive.sql("describe formatted userdata1").show(truncate = False)

sparkHive.sql("load data local inpath 'file:///home/mapr/vijayPractise/spark/data/parquet/userdata1.parquet'\
                 overwrite into table userdata1")

