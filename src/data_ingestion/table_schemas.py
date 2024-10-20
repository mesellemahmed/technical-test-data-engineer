from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType, TimestampType, DateType, ArrayType, BooleanType


class TableSchemas:
    
    track_bronze_schema = StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("artist", StringType(), True),
        StructField("songwriters", StringType(), True),
        StructField("duration", StringType(), True),
        StructField("genres", StringType(), True),
        StructField("album", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True)
        ])
    
    user_bronze_schema = StructType([
        StructField("id", LongType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("favorite_genres", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True)
        ])
    
    listen_history_bronze_schema = StructType([
        StructField("user_id", LongType(), True),
        StructField("items", ArrayType(IntegerType()), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True)
        ])
    
    track_silver_schema = StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("artist", StringType(), True),
        StructField("songwriters", StringType(), True),
        StructField("duration", LongType(), True),
        StructField("genres", StringType(), True),
        StructField("album", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("last_update_date", TimestampType(), True)
        ])
    
    artist_silver_schema = StructType([
        StructField("id", LongType(), True),
        StructField("artist", StringType(), True),
        StructField("last_update_date", TimestampType(), True)
        ])
    
    album_silver_schema = StructType([
        StructField("id", LongType(), True),
        StructField("album", StringType(), True),
        StructField("last_update_date", TimestampType(), True)
        ])
    
    genre_silver_schema = StructType([
        StructField("id", LongType(), True),
        StructField("genre", StringType(), True),
        StructField("last_update_date", TimestampType(), True)
        ])
    
    user_silver_schema = StructType([
        StructField("id", LongType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("favorite_genres", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("last_update_date", TimestampType(), True)
        ])
    
    listen_history_silver_schema = StructType([
        StructField("user_id", LongType(), True),
        StructField("items", ArrayType(IntegerType()), True),
        StructField("created_at", TimestampType(), True),
        StructField("last_update_date", TimestampType(), True)
        ])
    
    dim_track_gold_schema = StructType([
        StructField("key", LongType(), False),
        StructField("id", LongType(), False),
        StructField("name", StringType(), True),
        StructField("artist_key", LongType(), True),
        StructField("album_key", LongType(), True),
        StructField("duration", LongType(), True),
        StructField("genres", StringType(), True),
        StructField("effective_date", TimestampType(), False),
        StructField("expiration_date", DateType(), True),
        StructField("is_current", BooleanType(), False)
    ])

    dim_artist_gold_schema = StructType([
        StructField("key", LongType(), False),
        StructField("id", LongType(), False),
        StructField("artist", StringType(), True),
        StructField("effective_date", TimestampType(), False),
        StructField("expiration_date", TimestampType(), True),
        StructField("is_current", BooleanType(), False)
    ])

    dim_album_gold_schema = StructType([
        StructField("key", LongType(), False),
        StructField("id", LongType(), False),
        StructField("album", StringType(), True),
        StructField("effective_date", TimestampType(), False),
        StructField("expiration_date", TimestampType(), True),
        StructField("is_current", BooleanType(), False)
    ])

    dim_genre_gold_schema = StructType([
        StructField("key", LongType(), False),
        StructField("id", LongType(), False),
        StructField("name", StringType(), True),
        StructField("effective_date", TimestampType(), False),
        StructField("expiration_date", TimestampType(), True),
        StructField("is_current", BooleanType(), False)
    ])

    dim_user_gold_schema = StructType([
        StructField("key", LongType(), False),
        StructField("id", LongType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("favorite_genres", StringType(), True),
        StructField("effective_date", TimestampType(), False),
        StructField("expiration_date", TimestampType(), True),
        StructField("is_current", BooleanType(), False)
    ])

    
    dim_date_gold_schema = StructType([
        StructField("date_key", LongType(), False),
        StructField("time_key", LongType(), False),
        StructField("datetime_key", LongType(), False),
        StructField("datetime", TimestampType(), False),
        StructField("day", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("year", IntegerType(), False),
        StructField("quarter", IntegerType(), False),
        StructField("hour", IntegerType(), False),
        StructField("minute", IntegerType(), False),
        StructField("second", IntegerType(), False),
        StructField("millisecond", IntegerType(), False),
        StructField("microsecond", IntegerType(), False),
        StructField("hour_12", StringType(), False),
        StructField("hour_24", IntegerType(), False),
        StructField("am_pm", StringType(), False),
        StructField("time_of_day", StringType(), False),
        StructField("is_morning", BooleanType(), False),
        StructField("is_afternoon", BooleanType(), False),
        StructField("is_evening", BooleanType(), False),
        StructField("is_night", BooleanType(), False),
        StructField("is_business_hour", BooleanType(), False),
        StructField("day_of_week", IntegerType(), False),
        StructField("day_name", StringType(), False),
        StructField("day_name_short", StringType(), False),
        StructField("day_of_year", IntegerType(), False),
        StructField("day_suffix", StringType(), False),
        StructField("week_of_year", IntegerType(), False),
        StructField("week_of_month", IntegerType(), False),
        StructField("is_weekend", BooleanType(), False),
        StructField("week_begin_date", TimestampType(), False),
        StructField("week_end_date", TimestampType(), False),
        StructField("month_name", StringType(), False),
        StructField("month_name_short", StringType(), False),
        StructField("days_in_month", IntegerType(), False),
        StructField("month_begin_date", TimestampType(), False),
        StructField("month_end_date", TimestampType(), False),
        StructField("quarter_name", StringType(), False),
        StructField("quarter_begin_date", TimestampType(), False),
        StructField("quarter_end_date", TimestampType(), False),
        StructField("year_begin_date", TimestampType(), False),
        StructField("year_end_date", TimestampType(), False),
        StructField("is_leap_year", BooleanType(), False),
        StructField("fiscal_year", IntegerType(), False),
        StructField("fiscal_quarter", IntegerType(), False),
        StructField("fiscal_month", IntegerType(), False),
        StructField("is_last_day_of_month", BooleanType(), False),
        StructField("is_first_day_of_month", BooleanType(), False),
        StructField("is_last_day_of_quarter", BooleanType(), False),
        StructField("is_first_day_of_quarter", BooleanType(), False),
        StructField("is_last_day_of_year", BooleanType(), False),
        StructField("is_first_day_of_year", BooleanType(), False),
        StructField("is_start_of_hour", BooleanType(), False),
        StructField("is_end_of_hour", BooleanType(), False),
        StructField("hour_of_day_name", StringType(), False),
        StructField("calendar_season", StringType(), False),
        StructField("is_holiday", BooleanType(), False),
        StructField("holiday_name", StringType(), True),  # Peut être null
        StructField("timezone_offset", IntegerType(), False),
        StructField("is_dst", BooleanType(), False),
        StructField("previous_datetime_key", LongType(), False),
        StructField("next_datetime_key", LongType(), False),
        StructField("same_datetime_last_week", LongType(), False),
        StructField("same_datetime_last_month", LongType(), False),
        StructField("same_datetime_last_year", LongType(), False)
    ])



    fact_listen_history_gold_schema = StructType([
        StructField("user_key", LongType(), False),
        StructField("track_key", LongType(), False),
        StructField("datetime_key", LongType(), False)
    ])