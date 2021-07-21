import sys

from pyspark import SparkConf
from pyspark.sql import *

from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_csv_df, get_country_count

if __name__ == '__main__':
    conf = get_spark_app_config()
    spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()

    logger = Log4J(spark)
    logger.info("Starting Spark session")

    spark_df =  load_csv_df(spark,sys.argv[1])
    partitioned_df = spark_df.repartition(2)

    partitioned_df.createOrReplaceTempView("survey_tbl")
    count_df = spark.sql("select Country,count(1) as count from survey_tbl where Age <40 group by Country")
    count_df.show()
    #count_df = get_country_count(partitioned_df)
    #count_df.show()







    logger.info("Stopping Spark session")

    spark.stop()



