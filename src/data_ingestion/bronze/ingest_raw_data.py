import os
import logging
from datetime import datetime
from pyspark.sql.functions import to_date, max as spark_max, col, to_timestamp, lit, from_json
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from src.data_ingestion.utils.globals import global_spark_session
from src.data_ingestion.api.api_client import APIClient
from src.data_ingestion.table_schemas import TableSchemas

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


class BronzeLayerIngestion:

    def __init__(self):
        self.api_client = APIClient()
        self.table_names = ["user", "track", "listen_history"]
        self.api_endpoints = ["users", "tracks", "listen_history"]

    def start(self):
        logger.info(f"Starting raw data ingestion: {datetime.now()}")
        try:
            for table_name, api_endpoint in zip(self.table_names, self.api_endpoints):
                self._process_table(table_name, api_endpoint)
        except Exception as error:
            logger.error(f"Error during data ingestion process: {str(error)}", exc_info=True)
        logger.info(f"Raw data ingestion finished: {datetime.now()}")

    def _process_table(self, table_name: str, api_endpoint: str):
        logger.info(f"Processing table: {table_name}")
        try:
            bronze_data_directory = self._get_bronze_data_path(table_name)
            spark_bronze_data_uri = f"file:///{bronze_data_directory}"
            table_schema = self._get_schema_for_table("bronze", table_name)
            existing_data_frame = self._read_existing_data(spark_bronze_data_uri, table_schema)
            last_updated_date = self._get_max_date(existing_data_frame, "updated_at")
            filtered_existing_data_frame = existing_data_frame.filter(col("updated_at") > lit(last_updated_date))
            new_data_frame = self._fetch_and_process_data(api_endpoint, table_schema, last_updated_date)
            logger.info(f"New data fetched, resulting in {new_data_frame.count()} new records.")
            combined_data_frame = self._union_dataframes(filtered_existing_data_frame, new_data_frame)
            self._write_data_to_bronze(combined_data_frame, spark_bronze_data_uri)
        except Exception as e:
            logger.error(f"Error processing table '{table_name}': {str(e)}", exc_info=True)

    def _get_bronze_data_path(self, table_name: str) -> str:
        try:
            current_directory = os.path.dirname(os.path.abspath(__file__))
            bronze_data_directory = os.path.join(current_directory, "..", "..", "..", "data", "bronze", table_name)
            bronze_data_directory = os.path.normpath(bronze_data_directory)
            os.makedirs(bronze_data_directory, exist_ok=True)
            return bronze_data_directory
        except Exception as error:
            logger.error(f"Error getting bronze data path for table '{table_name}': {str(error)}", exc_info=True)
            raise

    def _get_schema_for_table(self, layer: str, table_name: str) -> StructType:
        try:
            schema_identifier = f"{table_name}_{layer}_schema"
            schema = getattr(TableSchemas, schema_identifier, None)
            if schema is None:
                raise ValueError(f"No schema found for '{schema_identifier}'")
            return schema
        except Exception as error:
            logger.error(f"Error getting schema for table '{table_name}': {str(error)}", exc_info=True)
            raise

    def _read_existing_data(self, file_path: str, schema: StructType) -> DataFrame:
        try:
            return global_spark_session().read.schema(schema=schema).parquet(file_path)
        except Exception as error:
            logger.error(f"Error reading existing data from path '{file_path}': {str(error)}", exc_info=True)
            raise

    def _get_max_date(self, data_frame: DataFrame, date_column_name: str) -> datetime:
        try:
            if date_column_name not in data_frame.columns:
                raise ValueError(f"Column '{date_column_name}' not found in the DataFrame")
            max_date = data_frame.select(spark_max(date_column_name)).first()[0]
            return max_date if max_date is not None else to_timestamp(lit("1900-01-01 00:00:00"))
        except Exception as e:
            logger.error(f"Error getting max date for column '{date_column_name}': {str(e)}", exc_info=True)
            raise

    def _fetch_and_process_data(self, table_name: str, schema: StructType, max_updated_datetime: datetime) -> DataFrame:
        try:
            current_page = 1
            combined_data_frame = None
            while True:
                response_data = self.api_client.get_data(table_name, current_page)
                if not response_data["items"]:
                    break
                data_frame = self._parse_json_to_spark_dataframe(response_data["items"], schema)
                filtered_data_frame = data_frame.filter(col("updated_at") > lit(max_updated_datetime))
                combined_data_frame = self._union_dataframes(combined_data_frame, filtered_data_frame)
                current_page += 1
            return combined_data_frame

        except Exception as error:
            logger.error(f"Error fetching and processing data for table '{table_name}': {str(error)}", exc_info=True)
            raise

    def _parse_json_to_spark_dataframe(self, json_data: dict, schema: StructType) -> DataFrame:
        try:
            spark_dataframe = global_spark_session().createDataFrame(json_data, schema=StructType([StructField(field.name, StringType(), True) for field in schema]))
            for field in schema.fields:
                if isinstance(field.dataType, ArrayType):
                    spark_dataframe = spark_dataframe.withColumn(field.name, from_json(col(field.name), field.dataType))
                else:
                    spark_dataframe = spark_dataframe.withColumn(field.name, col(field.name).cast(field.dataType))
            return spark_dataframe
        except Exception as error:
            logger.error(f"Error parsing JSON data to Spark DataFrame: {str(error)}", exc_info=True)
            raise

    def _union_dataframes(self, first_dataframe: DataFrame, second_dataframe: DataFrame) -> DataFrame:
        try:
            if first_dataframe is None:
                return second_dataframe
            if second_dataframe is None:
                return first_dataframe
            return first_dataframe.unionByName(second_dataframe, allowMissingColumns=True)
        except Exception as union_error:
            logger.error(f"Error unioning DataFrames: {str(union_error)}", exc_info=True)
            raise

    def _write_data_to_bronze(self, data_frame: DataFrame, destination_path: str):
        try:
            df_with_partition = data_frame.withColumn("partition_key", to_date("updated_at"))
            df_with_partition.write.partitionBy("partition_key").mode("append").parquet(destination_path)
            logger.info(f"Data written to {destination_path}")
        except Exception as write_error:
            logger.error(f"Error writing data to bronze layer for path '{destination_path}': {str(write_error)}", exc_info=True)
            raise
