from pyspark.sql import DataFrame
import json
import pandas

## Transformer Class ##
## This class has some attributes and methods which enables an open framework to process Spark DataFrame and Pandas

class Transformer ():
    
    def __init__(self, df: DataFrame):
        self.df = df
        


