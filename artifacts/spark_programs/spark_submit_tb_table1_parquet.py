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
    # Sample parameter values
    # params['hive_database_raw'] ='db_sap_ge2_raw_dev'
    # params['hive_table_raw'] ='tb_global_bkpf'
    # params['s3_object_raw'] = 's3://datalake-raw-dev/sap/ge2/global/financial/bkpf/'
    # params['hive_database_analytics'] = 'db_financial_dev'
    # params['hive_table_analytics'] = 'tb_global_bkpf_parquet'
    # params['s3_object_analytics'] ='s3://datalake-analytics-dev/global/financial/tb_global_bkpf_parquet/'

    # Check for all options. This program need all of them to run.
    param = dict()
    params = kwargs
    path, filename = os.path.split(__file__)
    name, ext = os.path.splitext(filename)
    config_file = os.path.join(path, name + '.json')

    # If there is a config file we parse from them
    if os.path.isfile(config_file):
        logger.info('Found a configuration file: {}, reading from them'.format(config_file))
        try:
            params = json.load(open(config_file))
        except ValueError:
            raise ValueError('Invalid JSON configuration file: {}'.format(config_file))

    for arg in kwargs:
        if kwargs.get(arg):
            logger.debug('Parsing parameter from command line {} : {}'.format(arg, kwargs.get(arg)))
            param[arg] = kwargs.get(arg)
        elif params.get(arg):
            logger.debug('Parsing parameter from config file {} : {}'.format(arg, params.get(arg)))
            param[arg] = params.get(arg)
        else:
            logger.info('Missing argument: {}'.format(arg))
            raise ValueError('Missing argument {}'.format(arg))

    logger.debug("hive_database_raw: {}".format(param['hive_database_raw']))
    logger.debug("hive_table_raw: {}".format(param['hive_table_raw']))
    logger.debug("s3_object_raw: {}".format(param['s3_object_raw']))
    logger.debug("hive_database_analytics: {}".format(param['hive_database_analytics']))
    logger.debug("hive_table_analytics: {}".format(param['hive_table_analytics']))
    logger.debug("s3_object_analytics: {}".format(param['s3_object_analytics']))

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

    df = spark.sql("SELECT * FROM {}.{} LIMIT 1".format(param['hive_database_raw'], param['hive_table_raw']))

    df.printSchema()

    df.show()

    schema = df.schema

    df2 = spark.read.option("sep", ";").option("header", "true").schema(schema).csv(param['s3_object_raw'])

    df2.printSchema()

    df2.show()

    def parse_date(argument, format_date='%Y-%m-%d'):
        try:
            return datetime.strptime(argument, format_date)
        except ValueError:
            return None

    convert_date = udf(lambda x: parse_date(x, '%Y.%m.%d'), DateType())

    df3 = df2.withColumn('year', year(convert_date(df2.date_time))).drop("month").drop("day")
    df3 = df3.repartition(1, df3['year'])
    df3.rdd.getNumPartitions()

    # df3.printSchema()

    # df3.show()

    df3.write.mode("append").insertInto(param['hive_database_analytics'] + "." + param['hive_table_analytics'])

    sc.stop()


if __name__ == '__main__':
    run()
