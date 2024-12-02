 #!/bin/bash
 # Get the directory of the script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

# Change directory to the script location
cd "$DIR"

# Execute the Python script
poetry build && poetry run spark-submit \
    --master local \
    --jars ./jars/postgresql-42.7.3.jar  \
    --py-files dist/transformations-*.whl \
    jobs/nyxa_ingest.py \
    --host localhost \
    --port 5432 \
    --username de_challenge \
    --password "Password1234**" \
    --database dw_flights
