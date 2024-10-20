import os
import sys
from pyspark.sql import SparkSession

class SparkManager:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    _spark_session = None  # Variable statique pour stocker la session Spark

    @staticmethod
    def get_spark_session():
        """
        Retourne une session Spark globale. Si elle n'est pas encore initialisée,
        la méthode la crée.
        """
        if SparkManager._spark_session is None:
            SparkManager._spark_session = SparkSession.builder \
                .appName("Global Spark Session") \
                .config("spark.sql.parquet.compression.codec", "snappy") \
                .config("spark.hadoop.io.native.lib.available", "false") \
                .config("spark.master", "local") \
                .getOrCreate()

        return SparkManager._spark_session

    @staticmethod
    def stop_spark_session():
        """Arrête la session Spark si elle est active."""
        if SparkManager._spark_session is not None:
            SparkManager._spark_session.stop()
            SparkManager._spark_session = None
