from scripts.minio_connect import MinioConnect
from config.minio_config import get_minio_config
from helpers.mino_helpers import MinioHelpers
from config.spark_config import SparkConnect

def main(config):
    jars = [
    "org.apache.hadoop:hadoop-aws:3.3.2",
    "com.amazonaws:aws-java-sdk-bundle:1.12.367",
    "io.delta:delta-core_2.12:2.4.0"

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

    minio_prc = MinioHelpers(config, spark = spark_connect.spark)
    minio_prc.create_bucket(bucket_name="raw")
    minio_prc.upload_data_to_bucket(bucket_name="raw")
    minio_prc.transform_data()
    minio_prc.delta_convert()


    


if __name__ == "__main__":
    config = get_minio_config()
    main(config)
