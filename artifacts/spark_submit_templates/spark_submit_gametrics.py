from __future__ import print_function

import sys
from datetime import datetime

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *

dt = str(sys.argv[1])
s3_object_name_stage = str(sys.argv[2])
hive_database = str(sys.argv[3])
hive_table = str(sys.argv[4])
s3_target = str(sys.argv[5])

# example:
# dt = "2017-10-31"
# s3_object_name_stage = "s3://it.centauro.odl.stage/doo/ga/ga_metrics/dt=2017-10-31/GA_2017_10_31_old.csv"
# hive_database = "odl_dl"
# hive_table = "tb_ga_metrics_parquet"
# s3_target = "s3://it.centauro.odl.dl/ga_metrics_parquet/"

print("dt: " + dt)
print("s3_object_name_stage: " + s3_object_name_stage)
print("hive_database: " + hive_database)
print("hive_table: " + hive_table)
print("s3_target: " + s3_target)

if __name__ == "__main__":
    sc = SparkContext()

    spark = SparkSession.builder.appName(s3_object_name_stage + 'AND' + dt).config(
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2").config("spark.speculation",
                                                                                    "false").config(
        "hive.exec.dynamic.partition", "true").config("hive.exec.dynamic.partition.mode",
                                                      "nonstrict").enableHiveSupport().getOrCreate()

    df = spark.read.option("header", "false").option("quote", "'").option("inferschema", "true").csv(
        s3_object_name_stage)

    df2 = df.selectExpr("_c0 as codigo1", "_c1 as codigo2", "_c2 as produto", "_c6 as data", "_c36 as data_compra")


    def parse_date(argument, format_date='%d/%m/%Y %H:%M:%S'):
        try:
            return datetime.strptime(argument, format_date)
        except:
            return None


    convert_date = udf(lambda x: parse_date(x, '%d/%m/%Y %H:%M:%S'), TimestampType())

    df3 = df2.withColumn('data_compra', convert_date(df2.data_compra))

    df4 = df3.withColumn('dt', df3['data_compra'].cast('date'))

    # insert into usefull for production environment
    df4.write.mode("append").insertInto(hive_database + "." + hive_table)

    # Create table usefull for dev environment to infer the schema and show create table on hive or athena
    # df4.write.partitionBy('dt').saveAsTable(hive_database + "." + hive_table, format='parquet', mode='append', path=s3_target)
