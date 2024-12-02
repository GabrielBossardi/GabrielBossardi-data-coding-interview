import logging
import sys
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, hour

def _map_airlines(df, target_table):
    try:
        transformed_df = df.select(
            col("carrier").alias("carrier"),
            col("name").alias("name")
        )
        logging.info(f"Successfully mapped fields of {target_table} to the target schema.")
        return transformed_df
    except Exception as e:
        logging.error(f"Error mapping fields: {e}")
        raise

def _map_airports(df, target_table):
    try:
        transformed_df = df.select(
            col("faa").alias("faa"),
            col("name").alias("name"),
            col("lat").cast("FLOAT").alias("latitude"),
            col("lon").cast("FLOAT").alias("longitude"),
            col("alt").cast("FLOAT").alias("altitude"),
            col("tz").cast("INTEGER").alias("timezone"),
            col("dst").alias("dst"),
            col("tzone").alias("timezone_name")
        )
        logging.info(f"Successfully mapped fields of {target_table} to the target schema.")
        return transformed_df
    except Exception as e:
        logging.error(f"Error mapping fields: {e}")
        raise

def _map_planes(df, target_table):
    try:
        transformed_df = df.select(
            col("tailnum").alias("tailnum"),
            col("year").cast("INTEGER").alias("year"),
            col("type").alias("type"),
            col("manufacturer").alias("manufacturer"),
            col("model").alias("model"),
            col("engines").cast("INTEGER").alias("engines"),
            col("seats").cast("INTEGER").alias("seats"),
            col("speed").cast("FLOAT").alias("speed"),
            col("engine").alias("engine")
        )
        logging.info(f"Successfully mapped fields of {target_table} to the target schema.")
        return transformed_df
    except Exception as e:
        logging.error(f"Error mapping fields: {e}")
        raise

def _map_weather(df, target_table):
    try:
        transformed_df = df.select(
            col("origin").alias("origin"),
            col("year").cast("INTEGER").alias("year"),
            col("month").cast("INTEGER").alias("month"),
            col()
            col("day").cast("INTEGER").alias("day"),
            col("hour").cast("INTEGER").alias("hour"),
            col("temp").cast("NUMERIC").alias("temp"),
            col("dewp").cast("NUMERIC").alias("dewp"),
            col("humid").cast("NUMERIC").alias("humid"),
            col("wind_dir").cast("FLOAT").alias("wind_dir"),
            col("wind_speed").cast("NUMERIC").alias("wind_speed"),
            col("wind_gust").cast("NUMERIC").alias("wind_gust"),
            col("precip").cast("NUMERIC").alias("precip"),
            col("pressure").cast("NUMERIC").alias("pressure"),
            col("visib").cast("NUMERIC").alias("visib"),
            col("time_hour").cast("TIMESTAMP").alias("time_hour")
        )
        logging.info(f"Successfully mapped fields of {target_table} to the target schema.")
        return transformed_df
    except Exception as e:
        logging.error(f"Error mapping fields: {e}")
        raise

def _map_flights(df, target_table):
    try:
        transformed_df = df.select(
            col("carrier").alias("carrier"),
            col("flight").cast("INTEGER").alias("flight"),
            col("year").cast("INT").alias("year"),
            col("month").cast("INT").alias("month"),
            col("day").cast("INT").alias("day"),
            col("hour").cast("INTEGER").alias("hour"),
            col("minute").cast("INTEGER").alias("minute"),
            col("dep_time").cast("INTEGER").alias("actual_dep_time"),
            col("sched_dep_time").cast("INTEGER").alias("sched_dep_time"),
            col("dep_delay").cast("INTEGER").alias("dep_delay"),
            col("arr_time").cast("INTEGER").alias("actual_arr_time"),
            col("sched_arr_time").cast("INTEGER").alias("sched_arr_time"),
            col("arr_delay").cast("INTEGER").alias("arr_delay"),
            col("tailnum").alias("tailnum"),
            col("origin").alias("origin"),
            col("dest").alias("dest"),
            col("air_time").cast("FLOAT").cast("FLOAT").alias("air_time"),
            col("distance").cast("FLOAT").cast("FLOAT").alias("distance"),
            col("time_hour").cast("TIMESTAMP").alias("time_hour")
        )
        logging.info(f"Successfully mapped fields of {target_table} to the target schema.")
        return transformed_df
    except Exception as e:
        logging.error(f"Error mapping fields: {e}")
        raise

def run(df: DataFrame, target_table: str):
    mapping_functions = {
        "airlines": _map_airlines,
        "airports": _map_airports,
        "planes": _map_planes,
        "weather": _map_weather,
        "flights": _map_flights
    }
    if target_table in mapping_functions:
        return mapping_functions[target_table](df, target_table)
    else:
        logging.error(f"No mapping method found for {target_table}")
        raise ValueError(f"No mapping method found for {target_table}")
