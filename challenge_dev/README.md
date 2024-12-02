# Project Overview

Welcome to the **BEON-Challenge** project. This project is designed to handle data transformations and ingestion using Apache Spark and dbt (data build tool). The project is structured to facilitate data processing for NYC Flights data, leveraging a PostgreSQL database for storage.

## Project Structure

- **src/spark**: Contains Spark jobs and transformation logic.
- **src/dbt/nyxa**: Houses dbt models, macros, and configurations.
- **infrastructure**: Contains Docker and database initialization scripts.

## Getting Started

### Prerequisites

- Python 3.11.10
- Apache Spark
- PostgreSQL
- Poetry for dependency management
- Docker (for containerized database setup)

### Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. **Install dependencies**:
   ```bash
   poetry install
   ```

3. **Set up the database**:
   - Use Docker to run a PostgreSQL instance:
     ```bash
     docker-compose -f infrastructure/Docker-compose.yml up -d
     ```

4. **Initialize the database**:
   - The `init_db.sql` script will automatically run on the first start of the PostgreSQL container to set up the necessary tables and users.

### Running the Project

#### Spark Ingestion Pipeline

To run the Spark ingestion pipeline, execute the following command:

```bash
./src/spark/spark-submit-scripts/run_script.sh
```


This command will ingest data from CSV files located in the `data/nyxa` directory into the PostgreSQL database.

#### dbt Project

The dbt project is configured to transform and test the ingested data. To run dbt commands, navigate to the `src/dbt/nyxa` directory and execute:

- **Run dbt models**:
  ```bash
  dbt run --profiles-dir .
  ```

- **Test dbt models**:
  ```bash
  dbt test --profiles-dir .
  ```

### Configuration

- **Database Configuration**: The database connection details are specified in `src/dbt/nyxa/profiles.yml`.
- **Spark Job Configuration**: The Spark job configurations are defined in `src/spark/jobs/nyxa_ingest.py`.

### Known Issues
- The nyc_weather.csv dataset contains duplicate rows for the primary key columns, specifically for the record: `EWR, 2013, 11, 3, 1`.

### Troubleshooting

- Ensure that the PostgreSQL service is running and accessible.
- Verify that all environment variables are correctly set, especially for Docker and dbt profiles.
- Check the logs in `src/spark/nyxa_ingest.log` and `src/dbt/logs/dbt.log` for any errors during execution.

### Resources

- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

## Authors

- Gabriel Bossardi - [contato@gabrielbossardi.com.br]

## License

This project is licensed under the MIT License - see the LICENSE file for details.
