from pyspark.sql import SparkSession
import sys
import psycopg2

try:
    spark = SparkSession.builder\
        .appName('Extract')\
        .getOrCreate()

    # Extract data from csv
    df = spark.read.csv(sys.argv[6], sep=',', header=True, inferSchema=True)

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

    # Create database if it doesn't exist
    sql_stmt = """
    CREATE TABLE IF NOT EXISTS source(
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

    # insert df into Database
    jdbc_url = "jdbc:postgresql://postgres-database:5432/data_people"
    df.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "source") \
        .option("user", "local") \
        .option("password", "password") \
        .mode("overwrite") \
        .save()

except Exception as e:
    print(e)
