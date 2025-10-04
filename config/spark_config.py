
from typing import Optional,List, Dict
from pyspark.sql import SparkSession
class SparkConnect:
    def __init__(self,
                 app_name:str,
                 master_url:str ="local[*]",
                 executor_memory:Optional[str]='4g',
                 executor_cores: Optional[int] = 2,
                 driver_memory: Optional[str]= '2g',
                 num_executors: Optional[int] = 2,
                 jar_packages: Optional[List[str]] = None,
                 spark_config: Optional[Dict[str,str]] = None,
                 log_level: str="INFO"):
        self.app_name = app_name
        self.master_url = master_url
        self.executor_memory = executor_memory
        self.executor_cores = executor_cores
        self.driver_memory = driver_memory
        self.num_executors = num_executors
        self.jar_packages = jar_packages
        self.spark_config = spark_config
        self.log_level = log_level
        self.spark=self.create_spark_session(self.master_url, self.executor_memory, self.executor_cores, 
                                             self.driver_memory,self.num_executors,self.jar_packages,
                                             self.spark_config, self.log_level)
        
    def create_spark_session(self,
                 master_url:str="local[*]",
                 executor_memory:Optional[str]='4g',
                 executor_cores: Optional[int] = 2,
                 driver_memory: Optional[str]= '2g',
                 num_executors: Optional[int] = 2,
                 jar_packages: Optional[List[str]] = None,
                 spark_config: Optional[Dict[str,str]] = None,
                 log_level: str="INFO") -> SparkSession:
        builder = SparkSession.builder\
            .appName(self.app_name)\
            .master(master_url)
        if executor_memory:
            builder.config("spark.executor.memory", executor_memory)
        if executor_cores:
            builder.config("spark.executor.cores", executor_cores)
        if driver_memory:
            builder.config("spark.driver.memory", driver_memory)
        if num_executors:
            builder.config("spark.executor.instances", num_executors)
        if jar_packages:
            jar_path=",".join([jar_package for jar_package in jar_packages])
            builder.config("spark.jars.packages", jar_path)
        if spark_config: 
            for key, value in spark_config.items():
                builder.config(key, value)

        spark= builder.getOrCreate()
        spark.sparkContext.setLogLevel(log_level)

        return spark
    
    def stop(self):
        if self.spark:
            self.spark.stop()
            print("---------Stop Spark Session--------")