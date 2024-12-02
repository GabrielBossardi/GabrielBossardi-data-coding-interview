import logging

def run(df, config, target_table):
    try:
        # Write the DataFrame to the target database table
        (df.write
            .format("jdbc")
            .option("url", f"jdbc:postgresql://{config.host}:{config.port}/{config.database}")
            .option("dbtable", target_table)
            .option("user", config.username)
            .option("password", config.password)
            .option("driver", "org.postgresql.Driver")
            .option("truncate", "true")
            .option("cascadeTruncate", "true")
            .mode("overwrite")
            .save())

        # Log the successful ingestion
        logging.info(f"Successfully ingested data into table {target_table}.")

    except Exception as e:
        logging.error(f"Error ingesting data into table {target_table}: {e}")
        raise
