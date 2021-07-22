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

    weather_data_df = load_csv_df(spark,"data/2019/*.csv.gz")
    weather_data_df = weather_data_df.withColumnRenamed("STN---", "STN")
    country_df = load_csv_df(spark,"data/countrylist.csv")
    station_df = load_csv_df(spark,"data/stationlist.csv")

    weather_data_df.createOrReplaceTempView("weather_tbl")
    country_df.createOrReplaceTempView("country_tbl")
    station_df.createOrReplaceTempView("station_tbl")

    average_mean_temp_sql = spark.sql("SELECT COUNTRY ,AVG_TEMP FROM \
                                       (SELECT COUNTRY_FULL AS COUNTRY           \
                                            ,AVG(TEMP)  AS AVG_TEMP              \
                                            ,DENSE_RANK() OVER( ORDER BY AVG(TEMP) DESC)  AS RNK \
                                                                                \
                                        FROM                                    \
                                        weather_tbl w                           \
                                        LEFT JOIN                               \
                                        station_tbl s                           \
                                        ON lpad(w.STN,6,0) = s.STN_NO           \
                                        LEFT JOIN                               \
                                        country_tbl c                           \
                                        ON s.COUNTRY_ABBR = c.COUNTRY_ABBR      \
                                        WHERE TEMP != 9999.9                     \
                                        GROUP BY COUNTRY_FULL                   \
                                        ORDER BY AVG_TEMP DESC )  SUB WHERE SUB.RNK=1 \
                                         ")
    #average_mean_temp_sql.show()
    average_mean_wind_sql= spark.sql("SELECT COUNTRY ,AVG_WIND FROM \
                                      (SELECT COUNTRY_FULL AS COUNTRY           \
                                            ,AVG(WDSP)  AS AVG_WIND      \
                                            ,DENSE_RANK() OVER( ORDER BY AVG(WDSP) DESC)  AS RNK              \
                                                                                \
                                        FROM                                    \
                                        weather_tbl w                           \
                                        LEFT JOIN                               \
                                        station_tbl s                           \
                                        ON lpad(w.STN,6,0) = s.STN_NO           \
                                        LEFT JOIN                               \
                                        country_tbl c                           \
                                        ON s.COUNTRY_ABBR = c.COUNTRY_ABBR      \
                                        WHERE WDSP != 999.9                    \
                                        GROUP BY COUNTRY_FULL                   \
                                        ORDER BY AVG_WIND DESC ) SUB WHERE SUB.RNK=2")

    #average_mean_wind_sql.show()

    most_consecutive_tornado  = spark.sql("SELECT COUNTRY_FULL AS COUNTRY         \
                                              ,YEARMODA  \
                                              ,FRSHTT          \
                                                                                \
                                        FROM                                    \
                                        weather_tbl w                           \
                                        LEFT JOIN                               \
                                        station_tbl s                           \
                                        ON lpad(w.STN,6,0) = s.STN_NO           \
                                        LEFT JOIN                               \
                                        country_tbl c                           \
                                        ON s.COUNTRY_ABBR = c.COUNTRY_ABBR      \
                                        WHERE FRSHTT =000001")

    most_consecutive_tornado.show()











    logger.info("Stopping Spark session")

    spark.stop()



