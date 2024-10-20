import logging
from datetime import datetime, timedelta
from pyspark.sql.functions import (max as spark_max)
from pyspark.sql import SparkSession
from pyspark.sql.functions import max as spark_max
from datetime import datetime, timedelta
from pyspark.sql.types import *
import calendar
from pytz import timezone
from src.data_ingestion.utils.globals import global_spark_session
from src.data_ingestion.table_schemas import TableSchemas

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DateTimeProcessor:
    def __init__(self, timezone_name='America/Toronto'):
        self.timezone_name = timezone_name

    def update_date_dimension(self,spark_gold_data_path, gold_data, timezone_name ='America/Toronto'):
        schema = getattr(TableSchemas, "dim_date_gold_schema")
        max_dt = self.get_max_datetime(gold_data)
        if max_dt.tzinfo is None:
           max_dt = timezone(timezone_name).localize(max_dt.replace(second=0, microsecond=0))
        end_dt = datetime.now(timezone(timezone_name))
        if max_dt >= end_dt:
            logger.info("La dimension datetime est à jour")
            return
        current_dt = max_dt + timedelta(minutes=1)
        records = []
        while current_dt <= end_dt:
            while len(records) < 3600:
                records.append(self.create_datetime_record(current_dt, timezone_name))
                current_dt += timedelta(minutes=1)
                if current_dt > end_dt:
                    logger.info("La date actuelle dépasse la date de fin.")
                    break
            if records:
                df = global_spark_session().createDataFrame(records, schema)
                df.write.mode("append").parquet(f"{spark_gold_data_path}")
                records = []

    def get_max_datetime(self, date_dataframe):
        max_dt = date_dataframe.agg(spark_max("datetime")).collect()[0][0]
        return max_dt if max_dt else datetime(2022, 11, 6, tzinfo=timezone(self.timezone_name))

    def create_datetime_record(self, dt, tz='UTC'):
        local_tz = timezone(tz)
        dt = dt.astimezone(local_tz)
        year_start = datetime(dt.year, 1, 1, tzinfo=local_tz)
        year_end = datetime(dt.year, 12, 31, 23, 59, 59, tzinfo=local_tz)
        month_start = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        month_end = dt.replace(day=calendar.monthrange(dt.year, dt.month)[1], hour=23, minute=59, second=59, microsecond=999999)
        week_start = dt - timedelta(days=dt.weekday())
        week_end = week_start + timedelta(days=6)
        quarter = (dt.month - 1) // 3 + 1
        quarter_start = datetime(dt.year, 3*quarter-2, 1, tzinfo=local_tz)
        quarter_end_month = 3 * quarter
        quarter_end = datetime(dt.year, quarter_end_month, calendar.monthrange(dt.year, quarter_end_month)[1], 23, 59, 59, tzinfo=local_tz)
        return{
            "date_key": int(dt.strftime('%Y%m%d')),
            "time_key": int(dt.strftime('%H%M%S')),
            "datetime_key": int(dt.strftime('%Y%m%d%H%M%S')),
            "datetime": dt,
            "day": dt.day,
            "month": dt.month,
            "year": dt.year,
            "quarter": quarter,
            "hour": dt.hour,
            "minute": dt.minute,
            "second": dt.second,
            "millisecond": dt.microsecond // 1000,
            "microsecond": dt.microsecond,
            "hour_12": dt.strftime('%I'),
            "hour_24": dt.hour,
            "am_pm": dt.strftime('%p'),
            "time_of_day": self.get_time_of_day(dt.hour),
            "is_morning": 6 <= dt.hour < 12,
            "is_afternoon": 12 <= dt.hour < 18,
            "is_evening": 18 <= dt.hour < 23,
            "is_night": dt.hour >= 23 or dt.hour < 6,
            "is_business_hour": 9 <= dt.hour < 17 and dt.weekday() < 5,
            "day_of_week": dt.weekday() + 1,
            "day_name": dt.strftime('%A'),
            "day_name_short": dt.strftime('%a'),
            "day_of_year": int(dt.strftime('%j')),
            "day_suffix": self.get_day_suffix(dt.day),
            "week_of_year": int(dt.strftime('%W')),
            "week_of_month": (dt.day - 1) // 7 + 1,
            "is_weekend": dt.weekday() >= 5,
            "week_begin_date": week_start,
            "week_end_date": week_end,
            "month_name": dt.strftime('%B'),
            "month_name_short": dt.strftime('%b'),
            "days_in_month": calendar.monthrange(dt.year, dt.month)[1],
            "month_begin_date": month_start,
            "month_end_date": month_end,
            "quarter_name": f"Q{quarter}",
            "quarter_begin_date": quarter_start,
            "quarter_end_date": quarter_end,
            "year_begin_date": year_start,
            "year_end_date": year_end,
            "is_leap_year": calendar.isleap(dt.year),
            "fiscal_year": dt.year,
            "fiscal_quarter": quarter,
            "fiscal_month": dt.month,
            "is_last_day_of_month": dt.day == calendar.monthrange(dt.year, dt.month)[1],
            "is_first_day_of_month": dt.day == 1,
            "is_last_day_of_quarter": dt.date() == quarter_end.date(),
            "is_first_day_of_quarter": dt.date() == quarter_start.date(),
            "is_last_day_of_year": dt.month == 12 and dt.day == 31,
            "is_first_day_of_year": dt.month == 1 and dt.day == 1,
            "is_start_of_hour": dt.minute == 0 and dt.second == 0,
            "is_end_of_hour": dt.minute == 59 and dt.second == 59,
            "hour_of_day_name": dt.strftime('%H:00'),
            "calendar_season": self.get_season(dt.month),
            "is_holiday": False,
            "holiday_name": None,
            "timezone_offset": int(dt.utcoffset().total_seconds() / 3600),
            "is_dst": dt.dst() != timedelta(0),
            "previous_datetime_key": int((dt - timedelta(days=1)).strftime('%Y%m%d%H%M%S')),
            "next_datetime_key": int((dt + timedelta(days=1)).strftime('%Y%m%d%H%M%S')),
            "same_datetime_last_week": int((dt - timedelta(weeks=1)).strftime('%Y%m%d%H%M%S')),
            "same_datetime_last_month": int(dt.replace(
                year=dt.year if dt.month > 1 else dt.year - 1,
                month=dt.month - 1 if dt.month > 1 else 12,
                day=min(dt.day, calendar.monthrange(
                    dt.year if dt.month > 1 else dt.year - 1,
                    dt.month - 1 if dt.month > 1 else 12)[1])
                    ).strftime('%Y%m%d%H%M%S')),
                    "same_datetime_last_year": int(dt.replace(
                        year=dt.year - 1,
                        day=min(dt.day, calendar.monthrange(dt.year - 1, dt.month)[1])
                        ).strftime('%Y%m%d%H%M%S'))
                        }

    def get_time_of_day(self, hour):
        if 6 <= hour < 12:
            return "Morning"
        elif 12 <= hour < 18:
            return "Afternoon"
        elif 18 <= hour < 23:
            return "Evening"
        else:
            return "Night"

    def get_day_suffix(self, day):
        if 10 <= day <= 20:
            return 'th'
        else:
            return {1: 'st', 2: 'nd', 3: 'rd'}.get(day % 10, 'th')

    def get_season(self, month):
        if month in [12, 1, 2]:
            return "Winter"
        elif month in [3, 4, 5]:
            return "Spring"
        elif month in [6, 7, 8]:
            return "Summer"
        else:
            return "Fall"
