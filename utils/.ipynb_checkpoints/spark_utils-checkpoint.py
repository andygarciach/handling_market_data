from pyspark.sql import SparkSession

class spark_utils():
    env : str
    appName : str
    
    def __init__(self, env: str, appName: str):
        self.env = env
        self.appName = appName
       

    @staticmethod
    def set_spark_session()->SparkSession.builder:
        spark = SparkSession.getActiveSession()
        if spark is not None:     
            return spark
        spark = SparkSession


    
    @staticmethod
    def create_spark_session(spark_session : SparkSession) -> SparkSession.builder:
        master : str = 'local'
        spark_session_builder = spark_session
        try:
            print("Environment: ",self.env)
            if self.env == "DEV" :
                print("Accedi a la condicion")
                spark_session_builder = SparkSession.builder.appName(self.appName).master(master)
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
        
                
                