from __future__ import print_function
import pyspark

import json
import logging
import os

import click

from datetime import datetime

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, to_timestamp, year, month, dayofmonth, concat, lit
from pyspark.sql.types import *
import time

s3_object_raw = 's3://customer-stage-dev/br/iba/laminacao/dt=2018-06-07/pda000_2018-06-07_12.31.08.txt'
# s3_object_raw = 's3://customer-stage-dev/br/iba/laminacao/dt=2018-06-07/'

hive_database = "db_stage_dev"
hive_database_analytics = "db_iba_dev"
hive_table = "tb_iba_laminacao"
hive_table_analytics = "tb_iba_laminacao_parquet"
s3_target = "s3://customer-datalake-dev/br/iba/tb_iba_laminacao_parquet/"

spark = SparkSession.builder.appName("Spark and Hive") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("spark.speculation", "false").config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport() \
    .getOrCreate()

# 512 MBs per partition
# sc._jsc.hadoopConfiguration().set("mapreduce.input.fileinputformat.split.minsize", "536870912")
# sc._jsc.hadoopConfiguration().set("mapreduce.input.fileinputformat.split.maxsize", "536870912")
# spark.conf.set("park.kryoserializer.buffer.max", "4096m")
# spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
# spark.conf.set("spark.rdd.compress", "true")
# spark.conf.set("spark.executor.memory", "10g")
spark.conf.set("spark.debug.maxToStringFields", "100")

# df = spark.read.option("sep", "\t").option("header", "true").option("inferschema", "true").csv(s3_object_raw)

df = spark.sql("SELECT * FROM {}.{} LIMIT 1".format(hive_database, hive_table))

schema = df.schema

df = spark.read.option("sep", "\t").option("header", "true").schema(schema).csv(s3_object_raw)

df.rdd.getNumPartitions()

df.printSchema()

df.show(1)

df = df.drop("dt")

def parse_date(argument, format_date='%d.%m.%Y %H:%M:%S.%f'):
    try:
        return datetime.strptime(argument, format_date)
    except:
        return None


convert_date = udf(lambda x: parse_date(x, '%d.%m.%Y %H:%M:%S.%f'), TimestampType())

df2 = df.withColumn('date_time', convert_date(df.date_time))

df2.printSchema()

df2.select('date_time').show(2, False)

df2.show()

df3 = df2.withColumn('dt', (df2.date_time).cast('date'))

df3.printSchema()

df3.show(1, False)

# df3.write.saveAsTable(hive_database_analytics + "." + hive_table_analytics, format='parquet', mode='append', path=s3_target)

df3.write.mode("append").insertInto(hive_database_analytics + "." + hive_table_analytics)
