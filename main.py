from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from utils import Utils
from utils.spark_utils import Spark_utils
import logging


def main():
    spark = None
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    spark_util = Spark_utils("DEV", "Meli_Pipeline")
    spark_util.set_spark_session()
    spark = spark_util.get_spark_session()
    
    logger.info("Reading Data Sources from Local Storage...")
    df_prints = spark.read.json("data_sources/prints.json")
    df_taps = spark.read.json("data_sources/taps.json")
    df_pays = spark.read.option("header","True").csv("data_sources/pays.csv")
    logger.info("3 New Datasets were created")


if __name__ == '__main__':
    main()