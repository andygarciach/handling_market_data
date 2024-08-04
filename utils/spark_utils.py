from pyspark.sql import SparkSession

class Spark_utils():

    def __init__(self, env: str, appName: str):
        self.env = env
        self.appName = appName
        self.spark = None

    
    def set_spark_session(self) -> SparkSession:
        self.spark = SparkSession.getActiveSession()
        if self.spark is not None:     
            return spark
        self.spark = self.__create_spark_session()    
        
    
    def __create_spark_session(self) -> SparkSession.builder:
        master : str = 'local'
        spark_session_builder = None
        print("Environment: ", self.env)
        try: 
            if self.env == "DEV" :
                spark_session_builder = SparkSession.builder.master(master).appName(self.appName)
        except Exception as e:
            print("Error al crear SparkSession: ", str(e))
        return spark_session_builder.getOrCreate()

    def get_spark_session(self):
        return self.spark

