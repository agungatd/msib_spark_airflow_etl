# Spark-Airflow

## steps to run the ETL dag:
1. update the `temp.env` file in dags directory then rename it to `.env`, e.g:
```.env
AIRFLOW_UID=197613
HOST='postgres-database'
PORT='5432'
DATABASE='data_people'
USER='local'
PASSWORD='password'
```
2. build dockerfile image:
```bash
docker build -t air-spark:0.1 .
```
3. run the containers:
```bash
docker compose up -d
```
4. go to airflow webUI in your browser: http://localhost:8080/
5. username and password should be `airflow` and `airflow` respectively.
6. add or update `spark_default` airflow connection in http://localhost:8080/connection/list/
    the values:
    - connection type: spark
    - host: spark://spark
    - port: 7077
7. run the dag `spark-test`
8. done!