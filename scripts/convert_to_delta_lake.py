from scripts.postgres_connect import PostgresConnect
from scripts.minio_connect import MinioConnect
from config.minio_config import get_postgres_config, get_minio_config
from pyspark.sql import SparkSession
from config.spark_config import SparkConnect
import traceback
from pyspark.sql.functions import year, month, date_format, dayofweek
from pyspark.sql import functions as F 

def main(minio_config, postgres_config, spark: SparkSession):
    def processing_dataframe(df, file_path):
        # Xác định đúng tên các cột thời gian dựa vào loại dữ liệu (yellow hoặc green)
        if 'yellow' in file_path:
            pickup_col = 'tpep_pickup_datetime'
            dropoff_col = 'tpep_dropoff_datetime'
        elif 'green' in file_path:
            pickup_col = 'lpep_pickup_datetime'
            dropoff_col = 'lpep_dropoff_datetime'
        else:
            raise ValueError("Không xác định được loại dữ liệu từ file path: nên chứa 'yellow' hoặc 'green'")

        # Add time-related columns
        df2 = df.withColumn('year', F.year(pickup_col)) \
                .withColumn('month', F.date_format(pickup_col, 'MMMM')) \
                .withColumn('dow', F.date_format(pickup_col, 'EEEE'))

        # Perform aggregation
        df_final = df2.groupBy(
            'year',
            'month',
            'dow',
            'vendorid',
            'ratecodeid',
            'pulocationid',
            'dolocationid',
            'payment_type',
            pickup_col,
            dropoff_col,
            'pickup_latitude',
            'pickup_longitude',
            'dropoff_latitude',
            'dropoff_longitude'
        ).agg(
            F.sum('passenger_count').alias('passenger_count'),
            F.sum('trip_distance').alias('trip_distance'),
            F.sum('extra').alias('extra'),
            F.sum('mta_tax').alias('mta_tax'),
            F.sum('fare_amount').alias('fare_amount'),
            F.sum('tip_amount').alias('tip_amount'),
            F.sum('tolls_amount').alias('tolls_amount'),
            F.sum('total_amount').alias('total_amount'),
            F.sum('improvement_surcharge').alias('improvement_surcharge'),
            F.sum('congestion_surcharge').alias('congestion_surcharge'),
        )

        # Rename columns
        df_final = df_final \
            .withColumn("pickup_latitude", F.col("pickup_latitude").cast("double")) \
            .withColumn("pickup_longitude", F.col("pickup_longitude").cast("double")) \
            .withColumn("dropoff_latitude", F.col("dropoff_latitude").cast("double")) \
            .withColumn("dropoff_longitude", F.col("dropoff_longitude").cast("double")) \
            .withColumn(pickup_col, F.col(pickup_col).cast("timestamp")) \
            .withColumn(dropoff_col, F.col(dropoff_col).cast("timestamp"))

        # Add service_type based on file path
        if 'yellow' in file_path:
            df_final = df_final.withColumn('service_type', F.lit(1))
        elif 'green' in file_path:
            df_final = df_final.withColumn('service_type', F.lit(2))

        return df_final

    bucket2 = "processed"
    with MinioConnect(minio_config["minio"].host, minio_config["minio"].port, minio_config["minio"].access_key, minio_config["minio"].secret_key) as minio_client:
        client, endpoint = minio_client.client, minio_client.endpoint
        response = client.list_objects(bucket2, prefix="", recursive=False)

        for obj in response:
            object_name = obj.object_name
            if object_name.endswith("/"):
                # folders.add(object_name)

                for file in client.list_objects(bucket2, prefix=object_name, recursive=True):
                    if file.object_name.endswith(".parquet"):
                        path = f"s3a://{bucket2}/{file.object_name}"

                        # read parquet by spark
                        df = spark.read.parquet(path)
                        print(f"--------Read successfully for {file.object_name}------------ ")
                        print(df.show(5, truncate=False))
                        df = processing_dataframe(df, file_path=path)
                        target_table = 'staging.nyc_taxi'
                        # write data into Postgres
                        if df and not df.rdd.isEmpty():
                            df.printSchema()
                            try:
                                df.write \
                                    .format("jdbc") \
                                    .option("url", f"jdbc:postgresql://{postgres_config.host}:{postgres_config.port}/{postgres_config.db}") \
                                    .option("dbtable", target_table) \
                                    .option("user", postgres_config.user_name) \
                                    .option("password", postgres_config.password) \
                                    .option("driver", "org.postgresql.Driver") \
                                    .mode("append") \
                                    .save()
                                print(f"Wrote {file.object_name} to Postgres")
                            except Exception as e:
                                print(f"Failed to write {file.object_name} to Postgres:")
                                traceback.print_exc()





    # paths = [
    #     f"s3a://{bucket2}/2024/",
    #     f"s3a://{bucket2}/2025/"
    # ]

    # df = spark.read.parquet(*paths)

    # print(f"--------Read successfully for {bucket2} bucket------------ ")
    # df = df.withColumn("year", year("lpep_pickup_datetime").cast("string"))
    # df = df.withColumn("month", month("lpep_pickup_datetime").cast("string"))
    # df = df.withColumn("dow", dayofweek("lpep_pickup_datetime").cast("string"))  # Trả về 1=Sunday,...7=Saturday
    # print(df.show(5, truncate=False))


    # target_table = 'staging.nyc_taxi'
    # # write data into Postgres
    # df.write \
    # .format("jdbc") \
    # .option("url", f"jdbc:postgresql://{postgres_config.host}:{postgres_config.port}/{postgres_config.db}") \
    # .option("dbtable", target_table) \
    # .option("user", postgres_config.user_name) \
    # .option("password", postgres_config.password) \
    # .option("driver", "org.postgresql.Driver") \
    # .mode("append") \
    # .save()
    # print(df.show(5, truncate=False))
    # print(f"--------Write {bucket2} successfully to postgres------------ ")


if __name__ == "__main__":
    config = get_minio_config()
    postgres_config = get_postgres_config()["postgres"]
    jars = [
    "org.apache.hadoop:hadoop-aws:3.3.2",
    "com.amazonaws:aws-java-sdk-bundle:1.12.367",
    "io.delta:delta-core_2.12:2.4.0",
    "org.postgresql:postgresql:42.2.27" 

    ]
    spark_connect= SparkConnect(
        app_name='tamdang_spark',
        master_url="local[*]",
        executor_memory='2g',
        executor_cores=1,
        driver_memory='2g',
        num_executors=1,
        log_level="INFO", 
        jar_packages=jars,
        spark_config = {
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.endpoint": config["minio"].endpoint,
            "spark.hadoop.fs.s3a.access.key": config["minio"].access_key,
            "spark.hadoop.fs.s3a.secret.key": config["minio"].secret_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        }
        
    )


    
    main(minio_config=config, postgres_config= postgres_config,spark=  spark_connect.spark)

