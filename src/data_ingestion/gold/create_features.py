import os
import logging
from datetime import datetime
from pyspark.sql.functions import (
    col, lit, when, to_date, row_number, to_timestamp, coalesce, explode, date_trunc, max as spark_max
)
from pyspark.sql.window import Window
from src.data_ingestion.utils.globals import global_spark_session
from src.data_ingestion.table_schemas import TableSchemas
from src.data_ingestion.utils.date_utils import DateTimeProcessor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)


class GoldLayerFeatures:

    def __init__(self):
        logger.info("Initializing Gold Layer Processing")
        self.dim_date = DateTimeProcessor("America/Toronto")
        self.dim_tables = ["artist", "album", "genre", "user", "track"]
        self.fact_tables = ["listen_history"]

    def start(self):
        logger.info(f"Starting Gold Layer processing: {datetime.now()}")
        try:
            self._process_dimensions()
            self._process_facts()
        except Exception as e:
            logger.error(f"An error occurred during Gold Layer processing: {str(e)}", exc_info=True)

    def _process_dimensions(self):
        gold_data_path = self._get_data_path("gold", "dim_date")
        spark_gold_data_path = f"file:///{gold_data_path}"
        gold_data = self._read_gold_data("dim_date", spark_gold_data_path)
        self.dim_date.update_date_dimension(spark_gold_data_path, gold_data)
        for dim in self.dim_tables:
            silver_data_path = self._get_data_path("silver", f"{dim}")
            gold_data_path = self._get_data_path("gold", f"dim_{dim}")
            spark_silver_data_path = f"file:///{silver_data_path}"
            spark_gold_data_path = f"file:///{gold_data_path}"
            self._process_dimension(dim, spark_silver_data_path, spark_gold_data_path)

    def _get_data_path(self, layer, table):
        try:
            current_path = os.path.dirname(os.path.abspath(__file__))
            data_path = os.path.join(current_path, "..", "..", "..", "data", layer, table)
            os.makedirs(data_path, exist_ok=True)
            return os.path.normpath(data_path)
        except Exception as e:
            logger.error(f"Error getting {layer} data path for table {table}: {str(e)}", exc_info=True)
            raise

    def _process_dimension(self, dim_name, spark_silver_data_path, spark_gold_data_path):
        logger.info(f"Processing dimension: {dim_name}")
        try:
            silver_df = self._read_silver_data(dim_name, spark_silver_data_path)
            gold_df = self._read_gold_data(f"dim_{dim_name}", spark_gold_data_path)
            new_records, updated_records = self._identify_changes(silver_df, gold_df, dim_name)
            if not new_records.isEmpty() or not updated_records.isEmpty():
                gold_df = self._apply_scd2(gold_df, new_records, updated_records, dim_name)
                self._write_gold_data(gold_df,spark_gold_data_path, f"dim_{dim_name}")
        except Exception as e:
            logger.error(f"Error processing dimension {dim_name}: {str(e)}", exc_info=True)

    def _read_silver_data(self, table_name, spark_silver_data_path):
        silver_schema = getattr(TableSchemas, f"{table_name}_silver_schema")
        return global_spark_session().read.schema(silver_schema).parquet(f"{spark_silver_data_path}")

    def _read_gold_data(self, table_name, spark_gold_data_path):
        gold_schema = getattr(TableSchemas, f"{table_name}_gold_schema")
        try:
            df = global_spark_session().read.schema(gold_schema).parquet(f"{spark_gold_data_path}")
            return df
        except Exception:
            logger.warning(f"No existing gold data found for {table_name}. Creating empty DataFrame.")
            empty_df = global_spark_session().createDataFrame([], gold_schema)
            return self._add_default_row(empty_df, table_name)

    def _add_default_row(self, df, table_name):
        default_timestamp = datetime(1970, 1, 1, 0, 0, 0)
        default_values = {
            "dim_track": (-1, -1, "Unknown", -1, -1, -1, None, default_timestamp, None, True),
            "dim_artist": (-1, -1, "Unknown", default_timestamp, None, True),
            "dim_album": (-1, -1, "Unknown", default_timestamp, None, True),
            "dim_genre": (-1, -1, "Unknown", default_timestamp, None, True),
            "dim_user": (-1, -1, "Unknown", "Unknown", "unknown@example.com", "Unknown", None, default_timestamp, None, True),
            "dim_date": (-1, -1, -1, None, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, "Unknown", "Unknown", False, False, False, False, False, -1, "Unknown", "Unk", -1, "Unknown", -1, -1, False, None, None, "Unknown", "Unk", -1, None, None, "Unknown", None, None, None, None, False, -1, -1, -1, False, False, False, False, False, False, False, False, "Unknown", "Unknown", False, "Unknown", 0, False, -1, -1, -1, -1, -1)

        }
        default_row = global_spark_session().createDataFrame([default_values[table_name]], schema=df.schema)
        return df.union(default_row)
    
    def _identify_changes(self, silver_df, gold_df, table_name):
        join_columns = self._get_join_columns(table_name)
        new_records = silver_df.join(gold_df, join_columns, "left_anti").orderBy("id")
        joined = silver_df.alias("s").join(gold_df.alias("g"), join_columns, "inner")
        updated_records = joined.filter(
            (col("s.last_update_date") > col("g.effective_date")) & 
            (col("g.is_current") == True)
        ).select("s.*", "g.effective_date")
        return new_records, updated_records

    def _get_join_columns(self, dim_name):
        join_columns = {
            "track": ["id"],
            "artist": ["id"],
            "album": ["id"],
            "genre": ["id"],
            "user": ["id"]
        }
        return join_columns[dim_name]
    
    def _apply_scd2(self, gold_df, new_records, updated_records, dim_name):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if not updated_records.isEmpty():
            gold_df = self._close_current_records(gold_df, updated_records, dim_name, now)
            gold_df = self._add_new_versions(gold_df, updated_records, dim_name, now)
        if not new_records.isEmpty():
            gold_df = self._add_new_records(gold_df, new_records, dim_name, now)
        gold_df = self._generate_surrogate_keys(gold_df, dim_name)
        return gold_df

    def _add_new_records(self, gold_df, new_records, dim_name, today):
        transformed_new_records = self._transform_silver_to_gold("dim", new_records, f"dim_{dim_name}")
        return gold_df.unionByName(transformed_new_records, allowMissingColumns=True)

    def _close_current_records(self, gold_df, updated_records, dim_name, today):
        return gold_df.withColumn(
            "expiration_date", 
            when((col("is_current") == True) & col("id").isin([r["id"] for r in updated_records.collect()]), today)
        ).withColumn("is_current", when(col("id").isin([r["id"] for r in updated_records.collect()]), False).otherwise(col("is_current")))

    def _add_new_versions(self, gold_df, updated_records, dim_name, today):
        updated_records = updated_records.withColumn("effective_date", lit(today))
        updated_records = updated_records.withColumn("expiration_date", lit(None))
        updated_records = updated_records.withColumn("is_current", lit(True))
        transformed_silver_df = self._transform_silver_to_gold("dim", updated_records, f"dim_{dim_name}")
        return gold_df.unionByName(transformed_silver_df, allowMissingColumns=True)

    def _transform_silver_to_gold(self, type, silver_dataframe, table_name):
        if type == "dim":
            common_fields = [
            lit(None).cast("bigint").alias("key")
            ,col(silver_dataframe.columns[0]).alias("id")
            ,to_timestamp(col("last_update_date")).alias("effective_date")
            ,lit(None).cast("date").alias("expiration_date")
            ,lit(True).alias("is_current")
            ]
            if table_name == "dim_artist":
                df= silver_dataframe.select(*common_fields,col("artist"))
                return df
            elif table_name == "dim_genre":
                df= silver_dataframe.select(*common_fields,col("genre")   )                        
                return df
            elif table_name == "dim_album":
                df= silver_dataframe.select(*common_fields,col("album")   )                        
                return df
            elif table_name == "dim_user":
                df= silver_dataframe.select(*common_fields  )                        
                return df
            elif table_name == "dim_track":
                artist_df = (self._read_gold_data(
                    "dim_artist", 
                    f"file:///{self._get_data_path('gold', 'dim_artist')}"
                )
                .filter(col("is_current") == True)
                .select(
                    col("artist"),
                    col("key").alias("artist_key")
                ))
                album_df = (self._read_gold_data(
                    "dim_album", 
                    f"file:///{self._get_data_path('gold', 'dim_album')}"
                )
                .filter(col("is_current") == True)
                .select(
                    col("album"),
                    col("key").alias("album_key")
                ))
                result_df = (silver_dataframe.alias("s")
                    .join(artist_df.alias("a"), col("s.artist") == col("a.artist"), "left")
                    .join(album_df.alias("al"), col("s.album") == col("al.album"), "left")
                    .select(
                        *common_fields,
                        col("s.name"),
                        col("a.artist_key"),col("al.album_key"),
                        col("s.duration"),
                        col("s.genres")
                    ))
                return result_df

    def _generate_surrogate_keys(self, gold_df, dim_name):
        window_spec = Window.orderBy("effective_date")
        max_surrogate_key = gold_df.agg(spark_max("key")).collect()[0][0]
        start_key = 1 if max_surrogate_key is None else max_surrogate_key + 1
        return gold_df.withColumn(
            "key",
            when(col("key").isNull(),
                row_number().over(window_spec) + start_key - 1
            ).otherwise(col("key"))
        )

    def _write_gold_data(self, df,spark_gold_data_path, table_name):
        df.write.mode("overwrite").parquet(f"{spark_gold_data_path}")
        logger.info(f"Data written to Gold layer: {table_name}")

    def _process_facts(self):
        for fact in self.fact_tables:
            silver_data_path = self._get_data_path("silver", fact)
            gold_data_path = self._get_data_path("gold", f"fact_{fact}")
            spark_silver_data_path = f"file:///{silver_data_path}"
            spark_gold_data_path = f"file:///{gold_data_path}"
            self._process_fact_table(fact, spark_silver_data_path, spark_gold_data_path)

    def _process_fact_table(self, fact_name, spark_silver_data_path, spark_gold_data_path):
        logger.info(f"Processing fact table: {fact_name}")
        try:
            silver_df = self._read_silver_data(fact_name, spark_silver_data_path)
            gold_df = self._read_gold_fact_data(f"fact_{fact_name}", spark_gold_data_path)
            gold_date_path = self._get_data_path("gold", "dim_date")
            spark_gold_date_path = f"file:///{gold_date_path}"
            dim_date = self._read_gold_data("dim_date", spark_gold_date_path)
            if not gold_df.isEmpty():
                max_date = self._get_fact_max_date(gold_df, dim_date)
            else:
                max_date = datetime.min
            new_facts = self._identify_new_facts(silver_df, gold_df, max_date)
            if not new_facts.isEmpty():
                new_facts_with_keys = self._add_surrogate_keys_fact( fact_name,new_facts, spark_gold_data_path)
                gold_df = gold_df.union(new_facts_with_keys)
                self._write_gold_data(gold_df, spark_gold_data_path, f"fact_{fact_name}")
            else:
                logger.info(f"No new facts for: {fact_name}")
        except Exception as e:
            logger.error(f"Error processing fact table {fact_name}: {str(e)}", exc_info=True)

    def _get_fact_max_date(self, gold_df, dim_date):
        gold_df = gold_df.join(
                dim_date.select("datetime_key", "datetime"),
                gold_df["datetime_key"] == dim_date["datetime_key"],
                "left"
            )
        if gold_df.isEmpty():
            return datetime.min
        else:
            return gold_df.select(spark_max("datetime")).first()[0]

    def _read_gold_fact_data(self, table_name, spark_gold_data_path):
        gold_schema = getattr(TableSchemas, f"{table_name}_gold_schema")
        try:
            return global_spark_session().read.schema(gold_schema).parquet(f"{spark_gold_data_path}")
        except Exception as e:
            logger.warning(f"No existing gold fact data found for {table_name}. Creating empty DataFrame.")
            return global_spark_session().createDataFrame([], gold_schema)

    def _identify_new_facts(self, silver_df, gold_df, max_date):
        if gold_df.isEmpty():
            return silver_df
        else:
            return silver_df.filter(col("created_at") > max_date)
        
    def _add_surrogate_keys_fact(self, fact_table, new_facts, fact_spark_gold_data_path):
        if fact_table == "listen_history":
            gold_data_path = self._get_data_path("gold", "dim_user")
            spark_gold_data_path = f"file:///{gold_data_path}"
            dim_user = self._read_gold_data("dim_user", spark_gold_data_path)  
            gold_data_path = self._get_data_path("gold", "dim_date")
            spark_gold_data_path = f"file:///{gold_data_path}"
            dim_date = self._read_gold_data("dim_date", spark_gold_data_path)
            gold_data_path = self._get_data_path("gold", "dim_track")
            spark_gold_data_path = f"file:///{gold_data_path}"
            dim_track = self._read_gold_data("dim_track", spark_gold_data_path)
            new_facts_with_user_key = new_facts.join(
                dim_user.select(col("key").alias("user_key"), 
                    col("id").alias("user_id")),
                "user_id",
                "left"
            ).withColumn(
                "user_key",
                coalesce(col("user_key"), lit(-1))
            )
            new_facts_with_user_key_transformed = new_facts_with_user_key.withColumn(
            "created_at_minute",
            date_trunc("minute", col("created_at"))
            )
            new_facts_with_user_key_date_key = new_facts_with_user_key_transformed.join(
                dim_date.select("datetime", "datetime_key"),
                to_date(new_facts_with_user_key_transformed["created_at_minute"]) == dim_date["datetime"],
                "left"
            ).withColumn(
                "date_key",
                coalesce(col("datetime_key"), lit(-1))
            )
            df_exploded = new_facts_with_user_key_date_key.select(
                "user_key",
                "date_key",
                explode("items").alias("track_id")
            )
            new_facts_with_user_key_date_key_track_key = df_exploded.join(
                dim_track.select(col("key").alias("track_key"), 
                    col("id").alias("track_id")),
                "track_id",
                "left"
            ).withColumn(
                "track_key",
                coalesce(col("track_key"), lit(-1))
            ).drop("track_id")
            return new_facts_with_user_key_date_key_track_key