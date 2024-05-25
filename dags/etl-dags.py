from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from dotenv import load_dotenv
import os

# argv
spark_master = "spark://spark:7077"
postgres_driver_jar = "/usr/share/java/postgresql-42.6.0.jar"
data_file = "/opt/data/raw.csv" #path in spark container

load_dotenv()
postgres_user = os.getenv('USER')
postgres_password = os.getenv('PASSWORD')
postgres_host = os.getenv('HOST')
postgres_port = os.getenv('PORT')
postgres_database = os.getenv('DATABASE')

dag = DAG(
        dag_id="spark-test",
        start_date=days_ago(0),
        schedule_interval="@daily",
        tags=["Test"]
    )

start = DummyOperator(task_id="start", dag=dag)

extract_data = SparkSubmitOperator(
    task_id="extract",
    application="/opt/airflow/dags/extract-spark.py",
    conf={"spark.master": spark_master},
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    application_args=[
        postgres_user,
        postgres_password,
        postgres_host,
        postgres_port,
        postgres_database,
        data_file
    ],
    conn_id="spark_default",
    dag=dag
)

transform_data = SparkSubmitOperator(
    task_id="transform",
    application="/opt/airflow/dags/transform-spark.py", 
    conf={"spark.master": spark_master},
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    application_args=[
        postgres_user,
        postgres_password,
        postgres_host,
        postgres_port,
        postgres_database
    ],
    conn_id="spark_default",
    dag=dag
)

load_data = SparkSubmitOperator(
    task_id="Load",
    application="/opt/airflow/dags/load-spark.py",
    conf={"spark.master": spark_master},
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    application_args=[
        postgres_user,
        postgres_password,
        postgres_host,
        postgres_port,
        postgres_database
    ],
    conn_id="spark_default",
    dag=dag
)


end = DummyOperator(task_id="end", dag=dag)

start >> extract_data >> transform_data >> load_data >> end
