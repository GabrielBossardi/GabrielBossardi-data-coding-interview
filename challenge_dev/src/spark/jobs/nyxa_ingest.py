import logging
import argparse
import sys
import os
from pyspark.sql import SparkSession
from transformations.nyxa import extract, map_fields, load

LOG_FILENAME = 'nyxa_ingest.log'
APP_NAME = "NYC Flights Ingestion Pipeline"

DATASOURCE_INGESTION = [
    {'input_file': 'nyc_airlines.csv', 'target_table': 'airlines', 'is_enabled': True},
    {'input_file': 'nyc_airports.csv', 'target_table': 'airports', 'is_enabled': True},
    {'input_file': 'nyc_planes.csv', 'target_table': 'planes', 'is_enabled': True},
    {'input_file': 'nyc_flights.csv', 'target_table': 'flights', 'is_enabled': True},
    {'input_file': 'nyc_weather.csv', 'target_table': 'weather', 'is_enabled': True},
]

DATASOURCE_PATH = '../../data/nyxa'

def _get_config(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', dest='host', type=str)
    parser.add_argument('--port', dest='port', type=int)
    parser.add_argument('--username', dest='username', type=str)
    parser.add_argument('--password', dest='password', type=str)
    parser.add_argument('--database', dest='database', type=str)
    parser.add_argument('--schema', dest='schema', type=str, default='public')
    config, _ = parser.parse_known_args(argv)

    return config

def _spark_init():
    spark = (SparkSession
                .builder
                .config("spark.jars", 'jars/postgresql-42.7.3.jar')
                .config("spark.driver.extraClassPath", 'jars/postgresql-42.7.3.jar')
                .appName(APP_NAME)
                .getOrCreate())
    sc = spark.sparkContext
    app_name = sc.appName
    return spark, sc, app_name

def main(argv):
    # Get config parameters
    config = _get_config(sys.argv)
    # Spark Init
    spark, _, _ = _spark_init()
    # Set Pipeline
    for datasource in DATASOURCE_INGESTION:
        if datasource['is_enabled']:
            # Construct the full path to the CSV file
            file_path = os.path.join(DATASOURCE_PATH, datasource['input_file'])

            # Extract datasources
            df_datasource = extract.run(spark, file_path)
            # Map original fields to target fields
            df_mapped = map_fields.run(df_datasource, datasource['target_table'])
            # Ingest Dataframe into the target table
            load.run(df_mapped, config, datasource['target_table'])

if __name__ == '__main__':
    logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO)
    logging.info(APP_NAME)
    logging.info('##### Data Source Ingestion Starting #####')
    main(sys.argv)
    logging.info('##### Data Source Ingestion Ending #####\n')
