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

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))
logger.info('Loading Job module.')


@click.command()
@click.option('-hdr', '--hive-database-raw', envvar='HIVE_DATABASE_RAW', help='Hive RAW database name')
@click.option('-htr', '--hive-table-raw', envvar='HIVE_TABLE_RAW', help='Hive RAW table name')
@click.option('-s3r', '--s3-object-raw', envvar='S3_OBJECT_RAW', help='S3 path for RAW objects')
@click.option('-hda', '--hive-database-analytics', envvar='HIVE_DATABASE_ANALYTICS',
              help='Hive Analytics database name')
@click.option('-hta', '--hive-table-analytics', envvar='HIVE_DATABASE_ANALYTICS', help='Hive Analytics table name')
@click.option('-s3a', '--s3-object-analytics', envvar='S3_OBJECT_ANALYTICS', help='S3 path for Analytics objects')
def run(**kwargs):
    
    sc = SparkContext()

    spark = SparkSession.builder.appName("Spark and Hive")\
                                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")\
                                .config("spark.speculation", "false")\
                                .config("hive.exec.dynamic.partition", "true")\
                                .config("hive.exec.dynamic.partition.mode", "nonstrict")\
                                .enableHiveSupport()\
                                .getOrCreate()

    # 512 MBs per partition
    sc._jsc.hadoopConfiguration().set("mapreduce.input.fileinputformat.split.minsize", "536870912")
    sc._jsc.hadoopConfiguration().set("mapreduce.input.fileinputformat.split.maxsize", "536870912")
    spark.conf.set("park.kryoserializer.buffer.max", "4096m")
    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    spark.conf.set("spark.rdd.compress", "true")
    spark.conf.set("spark.executor.memory", "10g")
    spark.conf.set("spark.debug.maxToStringFields", "100")

    df = spark.read.parquet()

    df.repartition()

    sc.stop()


if __name__ == '__main__':
    run()
