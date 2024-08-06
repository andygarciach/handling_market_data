from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from utils import Utils
from utils.Utils import Transformer
from utils.spark_utils import Spark_utils
import logging


def main():

    ## Settings variables for log handling and SparkSession
    spark = None
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    ## Spark_utils object to enable SparkSession
    spark_util = Spark_utils("DEV", "Meli_Pipeline")
    spark_util.set_spark_session()
    spark = spark_util.get_spark_session()

    ## Iniaiting data reads from original source to Dataframe
    logger.info("Reading Data Sources from Local Storage...")

    logger.info("Defining prints.json schema ...")
    prints_schema = StructType([
    StructField("day", DateType(), True),
    StructField("event_data", StructType([
        StructField("position", IntegerType(), True),
        StructField("value_prop", StringType(), True)
    ]), True),
    StructField("user_id", IntegerType(), True)
    ])

    ## Reading "prints.json" data to dataframe
    logger.info("Read prints.json with as dataset with proper schema")
    df_prints = spark.read.json("data_sources/prints.json", schema=prints_schema)

    logger.info("Prints's dataframe schema...")
    df_prints.printSchema()

    logger.info("Some prints's dataframe records...")
    df_prints.groupBy("user_id").count().show(10, truncate = False)

    ## Reading "taps.json" data to dataframe

    taps_schema = StructType([
    StructField("day", DateType(), True),
    StructField("event_data", StructType([
        StructField("position", IntegerType(), True),
        StructField("value_prop", StringType(), True)
    ]), True),
    StructField("user_id", IntegerType(), True)
    ])
    
    df_taps = spark.read.json("data_sources/taps.json")

    ## Reading "prints.json" data to dataframe
    df_pays = spark.read.option("header","True").csv("data_sources/pays.csv")

    logger.info("3 New Datasets were created")

    #################### Processing df_prints DataFrame ##########################

    logger.info("Calculating week of year for column day in prints")
    df_prints = Transformer.calcWeekOfTheYear(df_prints, "day", "week_of_year")

    df_prints.printSchema()

    logger.info("Calculating Rank for column week_of_year in prints")
    df_prints = Transformer.calcRank(df_prints, ['user_id'], ['week_of_year'], "rank_num", "desc")

    logger.info("Calculating Row_Number for column week_of_year in prints")
    df_prints = Transformer.calcRowNumber(df_prints, ['user_id','week_of_year'], ['week_of_year'], "row_num", "desc")
    
    df_prints.printSchema()

    logger.info("Showing processed data for Prints")
    df_prints.show(truncate = False)
    ##df_prints.show(10, truncate = False)

    #################### Processing df_taps DataFrame ##########################

    logger.info("Calculating week of year for column day in taps")
    df_taps = Transformer.calcWeekOfTheYear(df_taps, "day", "week_of_year")

    df_taps.printSchema()

    logger.info("Calculating Rank for column week_of_year in taps")
    df_taps = Transformer.calcRank(df_taps, ['user_id'], ['week_of_year'], "rank_num", "desc")

    logger.info("Calculating Row_Number for column week_of_year in prints")
    df_taps = Transformer.calcRowNumber(df_taps, ['user_id','week_of_year'], ['week_of_year'], "row_num", "desc")
    
    df_taps.printSchema()

    logger.info("Showing processed data for Prints")
    df_taps.show(truncate = False)
    ##df_prints.show(10, truncate = False)

    #################### Processing df_pays DataFrame ##########################

    logger.info("Calculating week of year for column day in pays")
    df_pays = Transformer.calcWeekOfTheYear(df_pays, "pay_date", "week_of_year")

    df_pays.printSchema()

    logger.info("Calculating Rank for column week_of_year in pays")
    df_pays = Transformer.calcRank(df_pays, ['user_id'], ['week_of_year'], "rank_num", "desc")

    logger.info("Calculating Row_Number for column week_of_year in prints")
    df_pays = Transformer.calcRowNumber(df_pays, ['user_id','week_of_year'], ['week_of_year'], "row_num", "desc")
    
    df_pays.printSchema()

    logger.info("Showing processed data for Prints")
    df_pays.show(truncate = False)
    ##df_prints.show(10, truncate = False)


if __name__ == '__main__':
    main()