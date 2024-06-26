from pyspark.sql import SparkSession
import psycopg2
import sys

spark = SparkSession.builder\
        .appName('Load')\
        .getOrCreate()

# set parameters
postgres_user = sys.argv[1]
postgres_password = sys.argv[2]
postgres_host = sys.argv[3]
postgres_port = sys.argv[4]
postgres_database = sys.argv[5]

# Connection database
conn = psycopg2.connect(
    host=postgres_host,
    port=postgres_port,
    database=postgres_database,
    user=postgres_user,
    password=postgres_password
)

sql_stmt = """
    CREATE TABLE IF NOT EXISTS final(
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            age VARCHAR(50),
            gender VARCHAR(50)
    );
"""
cursor = conn.cursor()
cursor.execute(sql_stmt)
conn.commit()
conn.close()

df_read = spark.read.jdbc(
    url=f'jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_database}',
    table="staging",
    properties={
        "user": "local",
        "password": "password",
        "driver": "org.postgresql.Driver"
        })
df_read.show()

df_read.write.format("jdbc") \
    .option("url", f'jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_database}') \
    .option("dbtable", "final") \
    .option("user", postgres_user) \
    .option("password", postgres_password) \
    .mode("overwrite") \
    .save()
