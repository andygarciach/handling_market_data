from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType
from pyspark.sql.window import Window
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
    
    df_taps = spark.read.json("data_sources/taps.json", schema = taps_schema)

    ## Reading "prints.json" data to dataframe

    pays_schema = StructType([
    StructField("pay_date", DateType(), True),
    StructField("total", DoubleType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("value_prop", StringType(), True)
    ])
    
    df_pays = spark.read.option("header","True").csv("data_sources/pays.csv", schema = pays_schema)

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

    ######### Processing the dataframes to calculate ########
    ### For each row in prints: 
    ###      how many times the user has been seen a value prop in the last 3 weeks.
    ###      Number of prints that the user clicked in the last 3 weeks for the same value prop.
    ###      Number of prints that the user paid in the last 3 weeks for the same value prop.
    ###      Total ammout paid by the user in the last 3 weeks for the same value prop.
    ### Finally, filter the dataset for the latest week prints per user. 

    ### There needs to define an specific lambda function to represent days as seconds. The intention of this is to calculate window frames between 21 days. (3 weeks in the past)
    ### The current row would not taken into account to calculate the number of clicks or pays in the last 3 weeks.
    days = lambda i: i * 86400 ## total number of seconds per day

    ### a Window is required to define the partition (per user, per print_label) and ordered by date. This date is a bigint (linux timestamp). 21 days represents 7 days per week (7 days * 3 weeks)
    window_spec = Window.partitionBy("a.user_id","a.value_prop").orderBy(col("date")).rangeBetween(days(-21), days(-1))

    ### New columns required for ordering and required value prop in prints.
    logger.info("Adding date, value_prop columns in Prints...")
    df_prints = df_prints.withColumn("date",to_timestamp(col("day")).cast("bigint"))
    df_prints = df_prints.withColumn("value_prop",(col("event_data.value_prop")))

    ### Calculating how many times a value prop has been seen in the last 3 weeks
    logger.info("Calculating number of records in Prints had been seen in the last 3 weeks...")
    df_prints = df_prints.alias("a").withColumn("count_print_last_3_weeks",count("a.user_id").over(window_spec))

    ### Joining df_prints and df_taps to identify the value_props that were clicked per user in the last 3 weeks.
    ### The number of clicked value props is calculated as well.
    logger.info("Linking prints and taps, identifying the print rows with clicks, calculating number of clicks in the last 3 weeks...")
    df_join = df_prints.alias("a").join(df_taps.alias("b"), (col("a.user_id") == col("b.user_id")) \
                                        & (col("a.event_data") == col("b.event_data")) \
                                        & (col("a.day") == col("b.day")), "leftouter") \
    .withColumn("clicked", when(expr("b.user_id is not null"), "Y").otherwise("N")) \
    .withColumn("count_click_last_3_weeks",count("b.user_id").over(window_spec)) \
    .select("a.*","clicked","count_click_last_3_weeks")

    ### Defining the result dataset with proper calculations.
    logger.info("Calculating number of pays and total paid in the last 3 weeks, preparing dataset to show...")
    dataset = df_join.join(df_pays.alias("c"), (col("a.day") == col("c.pay_date")) \
                           & (col("a.value_prop") == col("c.value_prop")) \
                           & (col("a.user_id") == col("c.user_id")), \
                           "leftouter") \
    .withColumn("count_pays_last_3_weeks", count("c.user_id").over(window_spec)) \
    .withColumn("sum_pays_last_3_weeks", sum("c.total").over(window_spec))

    # Retrieving results
    logger.info("Showing Results and dataset for Model consumpation:")
    dataset.select("a.*","clicked","count_click_last_3_weeks","count_pays_last_3_weeks","sum_pays_last_3_weeks").na.fill(0).where(col("a.rank_num")==1) \
    .show(100, truncate = False)

if __name__ == '__main__':
    main()