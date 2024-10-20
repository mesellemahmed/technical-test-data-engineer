from pyspark.sql import SparkSession, DataFrame

def init_global_spark_session(spark_session: SparkSession):
    global spark_session_global
    spark_session_global = spark_session

def global_spark_session()->SparkSession:
    return spark_session_global

def init_global_configuration(configuration):
    global configuration_global
    configuration_global = configuration

def global_configuration():
    return configuration_global