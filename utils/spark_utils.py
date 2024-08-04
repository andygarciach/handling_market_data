from pyspark.sql import SparkSession

class Spark_utils():

    def __init__(self, env: str, appName: str):
        self.env = env
        self.appName = appName

    @staticmethod
    def set_spark_session(self) -> SparkSession:
        spark = SparkSession.getActiveSession()
        if spark is not None:     
            return spark
        spark = self.__create_spark_session(spark)    
        return spark
    
    def create_spark_session(self, spark: SparkSession) -> SparkSession.builder:
        master : str = 'local'
        spark_session_builder = spark
        print("Environment: ", self.env)
        try: 
            if self.env == "DEV" :
                print("Accedi a la condicion")
                spark_session_builder = SparkSession.builder.master(master).appName(self.appName)
        except Exception as e:
            pass    
        return spark_session_builder

def get_spark_session(env, app_name):
    if env == 'DEV':
        spark = SparkSession. \
            builder. \
            master('local'). \
            appName(app_name). \
            getOrCreate()
        return spark
    elif env == 'PROD':
        spark = SparkSession. \
            builder. \
            master('yarn'). \
            appName(app_name). \
            getOrCreate()
        return spark
    return