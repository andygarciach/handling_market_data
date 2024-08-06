from pyspark.sql import DataFrame
from pyspark.sql.functions import col, weekofyear, rank, row_number
from pyspark.sql.window import Window
import json
import pandas

## Transformer Class ##
## This class has some attributes and methods which enables an open framework to process Spark DataFrame and Pandas

class Transformer ():
    
    @staticmethod
    def calcWeekOfTheYear (df : DataFrame,
                           sourceCol : str,
                           targetCol : str
                          ) -> DataFrame:
        df_proc = df
        try:
            df_proc = df_proc.withColumn(targetCol, weekofyear(col(sourceCol)))
        except Exception as e:
            print("Error when calculating week of year :", str(e))

        return df_proc

    @staticmethod
    def calcRank (df: DataFrame,
                  partitionCols : list,
                  orderByCols : list,
                  targetCol : str,
                  orderAs : str
                 ) -> DataFrame:
        
        df_proc = df
        
        try:
            
            if orderAs == "asc":
                ##window = Window.partitionBy(*[col(c) for c in partitionCols]).orderBy(*[col(c).asc() for c in orderByCols])
                window = Window.orderBy(*[col(c).asc() for c in orderByCols])
            elif orderAs == "desc":
                ##window = Window.partitionBy(*[col(c) for c in partitionCols]).orderBy(*[col(c).desc() for c in orderByCols])
                window = Window.orderBy(*[col(c).desc() for c in orderByCols])
            else:
                print("Expected 'asc' or 'desc' as order type for Rank Function") 
            
            df_proc = df_proc.withColumn(targetCol, rank().over(window))
        
        except Exception as e:
            print("Error when calculating Rank Function: ", str(e))

        return df_proc

    @staticmethod
    def calcRowNumber (df: DataFrame,
                  partitionCols : list,
                  orderByCols : list,
                  targetCol : str,
                  orderAs : str
                 ) -> DataFrame:
        
        df_proc = df
        
        try:
            
            if orderAs == "asc":
                window = Window.partitionBy(*[col(c) for c in partitionCols]).orderBy(*[col(c).asc() for c in orderByCols])
                ##window = Window.orderBy(*[col(c).asc() for c in orderByCols])
            elif orderAs == "desc":
                window = Window.partitionBy(*[col(c) for c in partitionCols]).orderBy(*[col(c).desc() for c in orderByCols])
                ##window = Window.orderBy(*[col(c).desc() for c in orderByCols])
            else:
                print("Expected 'asc' or 'desc' as order type for Rank Function") 
            
            df_proc = df_proc.withColumn(targetCol, row_number().over(window))
        
        except Exception as e:
            print("Error when calculating Row_Number Function: ", str(e))

        return df_proc

            
        
        
        
        


