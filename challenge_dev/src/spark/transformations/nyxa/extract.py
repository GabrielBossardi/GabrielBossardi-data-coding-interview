import logging
from pyspark.sql import SparkSession

def run(spark: SparkSession, file_path: str):
    try:
        # Read the CSV file without schema enforcement
        df = spark.read.option("delimiter", ",").csv(file_path, header=True)

        # Log the successful extraction
        logging.info(f"Successfully extracted data from {file_path}")

        return df

    except Exception as e:
        logging.error(f"Error extracting data from {file_path}: {e}")
        raise
