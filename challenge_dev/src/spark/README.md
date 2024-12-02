```bash
poetry build && poetry run spark-submit \
    --master local \
    --jars ./jars/postgresql-42.7.3.jar  \
    --py-files dist/transformations-*.whl \
    jobs/nyxa_ingest.py \
    --host localhost \
    --port 5432 \
    --username postgres \
    --password "beon123" \
    --database dw_flights
```
