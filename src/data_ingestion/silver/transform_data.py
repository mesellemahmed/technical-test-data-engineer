import os
import logging
from datetime import datetime
from pyspark.sql.functions import to_date, when, regexp_replace, trim, lower, explode, current_timestamp, max as spark_max, col, to_timestamp, lit, split, expr, monotonically_increasing_id
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from functools import reduce
from src.data_ingestion.utils.globals import global_spark_session
from src.data_ingestion.table_schemas import TableSchemas

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SilverLayerTransformation:

    def __init__(self):
        logger.info("Initializing Silver Layer Processing")
        self.target_tables = [
             "track", "user", "listen_history", "album",
              "artist"
               , "genre"
            ]

    def start(self):
        logger.info(f"Starting Silver Layer processing: {datetime.now()}")
        try:
            for table in self.target_tables:
                self._process_table(table)
            logger.info(f"Finished Silver Layer processing: {datetime.now()}")
        except Exception as error:
            logger.error(f"An error occurred during Silver Layer processing: {str(error)}", exc_info=True)

    def _process_table(self, table: str):
        logger.info(f"Processing table: {table}")
        try:
            bronze_data_path = self._get_bronze_data_path(table)
            silver_data_path = self._get_silver_data_path(table)
            spark_bronze_data_path = f"file:///{bronze_data_path}"
            spark_silver_data_path = f"file:///{silver_data_path}"
            silver_schema = self._get_schema_for_table("silver", table)
            existing_silver_layer_data = self._read_existing_data(spark_silver_data_path, silver_schema)
            latest_update_timestamp = self._get_max_date(existing_silver_layer_data, "last_update_date")
            filtred_old_silver_data = existing_silver_layer_data.filter(col("last_update_date") > lit(latest_update_timestamp))
            if table in ["album", "artist", "genre"]:
                new_data = self._process_derived_table(table)
            else:
                bronze_schema = self._get_schema_for_table("bronze", table)
                new_bronze_data = self._read_bronze_data(spark_bronze_data_path, bronze_schema, latest_update_timestamp)
                new_data = self._apply_transformations(new_bronze_data, table)
            final_data = self._union_dataframes(filtred_old_silver_data, new_data)
            self._write_data_to_silver(final_data, spark_silver_data_path)
        except Exception as error:
            logger.error(f"An error occurred while processing table {table}: {str(error)}", exc_info=True)

    def _get_bronze_data_path(self, table: str) -> str:
        try:
            current_path = os.path.dirname(os.path.abspath(__file__))
            bronze_data_path = os.path.join(current_path, "..", "..", "..", "data", "bronze", table)
            return os.path.normpath(bronze_data_path)
        except Exception as e:
            logger.error(f"Error getting bronze data path for table {table}: {str(e)}", exc_info=True)
            raise

    def _get_silver_data_path(self, table: str) -> str:
        try:
            current_path = os.path.dirname(os.path.abspath(__file__))
            silver_data_path = os.path.join(current_path, "..", "..", "..", "data", "silver", table)
            os.makedirs(silver_data_path, exist_ok=True)
            return silver_data_path
        except Exception as e:
            logger.error(f"Error getting silver data path for table {table}: {str(e)}", exc_info=True)
            raise

    def _get_schema_for_table(self, layer: str, table_name: str) -> StructType:
        try:
            schema_name = f"{table_name}_{layer}_schema"
            schema = getattr(TableSchemas, schema_name, None)
            if schema is None:
                raise ValueError(f"No schema found for '{schema_name}'")
            return schema
        except Exception as e:
            logger.error(f"Error getting schema for table {table_name} in layer {layer}: {str(e)}", exc_info=True)
            raise

    def _read_existing_data(self, path: str, schema: StructType) -> DataFrame:
        try:
            return global_spark_session().read.schema(schema=schema).parquet(path)
        except Exception as e:
            logger.warning(f"No existing data found at {path}. Creating empty DataFrame. Error: {str(e)}")
            return global_spark_session().createDataFrame([], schema)

    def _read_bronze_data(self, path: str, schema: StructType, max_updated_date: datetime) -> DataFrame:
        try:
            return (global_spark_session().read.schema(schema=schema)
                    .parquet(path)
                    .filter(col("updated_at") > max_updated_date))
        except Exception as e:
            logger.error(f"Error reading bronze data from {path}: {str(e)}", exc_info=True)
            raise

    def _get_max_date(self, df: DataFrame, date_column: str) -> datetime:
        try:
            if date_column not in df.columns:
                raise ValueError(f"Column '{date_column}' not found in the DataFrame")
            max_date = df.select(spark_max(date_column)).first()[0]
            return max_date if max_date is not None else to_timestamp(lit("1900-01-01 00:00:00"))
        except Exception as e:
            logger.error(f"Error getting max date from column {date_column}: {str(e)}", exc_info=True)
            raise

    def _apply_transformations(self, df: DataFrame, table: str) -> DataFrame:
        try:
            transformations = {
                "track": self._get_tracks_transformations(),
                "user": self._get_users_transformations(),
                "listen_history": self._get_listen_history_transformations()
            } 
            if table not in transformations:
                raise ValueError(f"Unrecognized table: {table}")
            return reduce(lambda df, f: f(df), transformations[table], df)
        except Exception as e:
            logger.error(f"Error applying transformations for table {table}: {str(e)}", exc_info=True)
            raise

    def _get_tracks_transformations(self):
        return [
            lambda df: df.withColumn("name", trim(regexp_replace(col("name"), r"[^\w\s]", ""))),
            lambda df: df.withColumn("artist", trim(col("artist"))),
            lambda df: df.withColumn("genre", lower(col("genres"))),
            lambda df: df.withColumn("duration_parts", split(col("duration"), ":")),
            lambda df: df.withColumn("duration_minutes", col("duration_parts").getItem(0).cast("bigint")),
            lambda df: df.withColumn("duration_seconds", col("duration_parts").getItem(1).cast("bigint")),
            lambda df: df.withColumn("duration_total_seconds", expr("duration_minutes * 60 + duration_seconds")),
            lambda df: df.drop("duration", "duration_parts", "duration_minutes", "duration_seconds"),
            lambda df: df.withColumnRenamed("duration_total_seconds", "duration"),
            lambda df: df.withColumnRenamed("updated_at", "last_update_date")
        ]

    def _get_users_transformations(self):
        return [
            lambda df: df.withColumn("first_name", trim(regexp_replace(col("first_name"), r"[^\w\s]", ""))),
            lambda df: df.withColumn("last_name", trim(regexp_replace(col("last_name"), r"[^\w\s]", ""))),
            lambda df: df.withColumn("email", lower(trim(col("email")))),
            lambda df: df.withColumn("gender", when(col("gender").isin("Male", "Female"), col("gender")).otherwise("Other")),
            lambda df: df.withColumn("favorite_genres", lower(col("favorite_genres"))),
            lambda df: df.withColumnRenamed("updated_at", "last_update_date")
        ]

    def _get_listen_history_transformations(self):
        return [
            lambda df: df.withColumnRenamed("updated_at", "last_update_date")
        ]

    def _process_derived_table(self, table: str) -> DataFrame:
        try:
            if table == "album":
                return self._process_albums()
            elif table == "artist":
                return self._process_artists()
            elif table == "genre":
                return self._process_genres()
            else:
                raise ValueError(f"Unrecognized derived table: {table}")
        except Exception as e:
            logger.error(f"Error processing derived table {table}: {str(e)}", exc_info=True)
            raise

    def _process_albums(self) -> DataFrame:
        try:
            tracks_df = self._read_existing_data(self._get_silver_data_path("track"), self._get_schema_for_table("silver", "track"))
            existing_albums_df = self._read_existing_data(self._get_silver_data_path("album"), self._get_schema_for_table("silver", "album"))
            new_albums_df = tracks_df.select("album").distinct()
            new_albums = new_albums_df.join(existing_albums_df, "album", "left_anti")
            max_id = existing_albums_df.agg({"id": "max"}).collect()[0][0]
            if max_id is None:
                max_id = 0
            if not new_albums.isEmpty():
                new_albums_with_id = new_albums.withColumn("id", (monotonically_increasing_id()+max_id+1).cast("bigint")) \
                                            .withColumn("last_update_date", current_timestamp()) 
                return new_albums_with_id
            else:
                return global_spark_session().createDataFrame([], existing_albums_df.schema)
        except Exception as e:
            logger.error(f"Error processing albums table: {str(e)}", exc_info=True)
            raise

    def _process_artists(self) -> DataFrame:
        try:
            tracks_df = self._read_existing_data(self._get_silver_data_path("track"), self._get_schema_for_table("silver", "track"))
            existing_artists_df = self._read_existing_data(self._get_silver_data_path("artist"), self._get_schema_for_table("silver", "artist"))
            new_artists_df = tracks_df.select("artist").distinct()
            new_artists = new_artists_df.join(existing_artists_df, "artist", "left_anti")
            max_id = existing_artists_df.agg({"id": "max"}).collect()[0][0]
            if max_id is None:
                max_id = 0
            if not new_artists.isEmpty():
                new_artists_with_id = new_artists.withColumn("id", (monotonically_increasing_id()+max_id+1).cast("bigint")) \
                                                .withColumn("last_update_date", current_timestamp())
                return new_artists_with_id
            else:
                return global_spark_session().createDataFrame([], existing_artists_df.schema)
        except Exception as e:
            logger.error(f"Error processing artists table: {str(e)}", exc_info=True)
            raise

    def _process_genres(self) -> DataFrame:
        try:
            tracks_df = self._read_existing_data(self._get_silver_data_path("track"), self._get_schema_for_table("silver", "track"))
            users_df = self._read_existing_data(self._get_silver_data_path("user"), self._get_schema_for_table("silver", "user"))
            existing_genres_df = self._read_existing_data(self._get_silver_data_path("genre"), self._get_schema_for_table("silver", "genre"))
            track_genres = tracks_df.select(explode(split(col("genres"), ",")).alias("genre")).distinct()
            user_genres = users_df.select(explode(split(col("favorite_genres"), ",")).alias("genre")).distinct()
            all_new_genres = track_genres.union(user_genres).distinct()
            new_genres = all_new_genres.join(existing_genres_df, "genre", "left_anti")
            max_id = existing_genres_df.agg({"id": "max"}).collect()[0][0]

            
            if max_id is None:
                max_id = 0
            if not new_genres.isEmpty():
                new_genres_with_id = new_genres.withColumn("id", (monotonically_increasing_id()+max_id+1).cast("bigint")) \
                                            .withColumn("last_update_date", current_timestamp())
                return new_genres_with_id
            else:
                return global_spark_session().createDataFrame([], existing_genres_df.schema)

        except Exception as e:
            logger.error(f"Error processing genres table: {str(e)}", exc_info=True)
            raise

    def _union_dataframes(self, first_dataframe: DataFrame, second_dataframe: DataFrame) -> DataFrame:
        try:
            if first_dataframe.isEmpty():
                return second_dataframe
            if second_dataframe.isEmpty():
                return first_dataframe
            return first_dataframe.unionByName(second_dataframe, allowMissingColumns=True)
        except Exception as e:
            logger.error(f"Error unioning DataFrames: {str(e)}", exc_info=True)
            raise

    def _write_data_to_silver(self, dataframe: DataFrame, path: str):
        try:
            df_with_partition = dataframe.withColumn("partition_key", to_date("last_update_date"))
            df_with_partition.write.partitionBy("partition_key").mode("append").parquet(path)
            logger.info(f"Data written to {path}")
        except Exception as error:
            logger.error(f"Error writing data to silver layer at {path}: {str(error)}", exc_info=True)
            raise