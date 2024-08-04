from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from utils import Utils
from utils.spark_utils import Spark_utils

import logging

def main():
    ##spark = spark_utils.get_spark_session("DEV", "Meli_Datapipeline")

    spark = Spark_utils("DEV", "Meli_Pipeline").set_spark_session(self)
    spark.getOrCreate()
    
if __name__ == '__main__':
    main()