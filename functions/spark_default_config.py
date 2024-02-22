from pyspark.sql import SparkSession



def spark_default_config(spark: SparkSession) -> None:
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")