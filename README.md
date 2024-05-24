# Spark-Airflow

# steps to run:
1. build dockerfile image:
```bash
docker build -t air-spark:0.1 .
```
2. run the containers:
```bash
docker compose up -d
```
3. run airflow webui: http://localhost:8080/
4. username and password should be `airflow` and `airflow` respectively.
5. add or update `spark_default` airflow connection in http://localhost:8080/connection/list/
    the values:
    - connection type: spark
    - host: spark://spark
    - port: 7077
6. run the dag `spark-test`
7. done!