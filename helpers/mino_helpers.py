
from scripts.minio_connect import MinioConnect
from typing import Dict
import os
from pyspark.sql import SparkSession
import pandas as pd
import pyarrow.parquet as pq
import tempfile
from pyspark.sql.types import *
class MinioHelpers:
    def __init__(self,minio_config:Dict, spark:SparkSession):
        self.minio_config = minio_config
        self.spark = spark

    def get_minio_client(self):
        with MinioConnect(self.minio_config["minio"].host, self.minio_config["minio"].port, self.minio_config["minio"].access_key, self.minio_config["minio"].secret_key) as minio_client:
            client, endpoint = minio_client.client, minio_client.endpoint
            return client, endpoint

    def create_bucket(self, bucket_name):   
        client,_ = self.get_minio_client()
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"--------Created bucket: {bucket_name} successfully-------")
        else:
            print(f"--------Bucket: {bucket_name} existed-------")
            
    
    def upload_data_to_bucket(self, bucket_name):
            client ,_ = self.get_minio_client()
            root_folder = "data"
            for year in os.listdir(root_folder):
                full_path = os.path.join(root_folder, year)
                if os.path.isdir(full_path):
                    for file in os.listdir(full_path):
                        file_path = os.path.join(full_path,file)
                        if os.path.isfile(file_path):
                            object_name = f'{year}/{file}'

                            try:
                                client.stat_object(bucket_name = bucket_name, object_name = object_name)
                                print(f"-----Skip {object_name} is existed in bucket {bucket_name}")
                            except Exception as e :
                                client.fput_object(bucket_name=bucket_name,object_name=object_name, file_path=file_path)
                                print(f"------Upload {file} to bucket {bucket_name}/{object_name}")
    

    def transform_data(self):
        
        def drop_column (df,file):
            """
            Drop columns 'store_and_fwd_flag'
            """
            if "store_and_fwd_flag" in df.columns:
                df = df.drop(columns=["store_and_fwd_flag"])
                print("Dropped column store_and_fwd_flag from file: " + file)
            else:
                print("Column store_and_fwd_flag not found in file: " + file)

            return df
        


        def merge_taxi_zone(df, file):
            """
                Merge dataset with taxi zone lookup
            """
            taxi_lookup_path = "helpers/taxi_lookup.csv"
            df_lookup = pd.read_csv(taxi_lookup_path)
            print("-----Read successfully df_lookup------")

            def merge_and_rename(df, location_id, lat_col, long_col):
                df = df.merge(df_lookup, left_on=location_id, right_on="LocationID")
                df = df.drop(columns=["LocationID", "Borough", "service_zone", "zone"])
                df = df.rename(columns={
                    "latitude" : lat_col,
                    "longitude" : long_col
                })
                return df

            if "pickup_latitude" not in df.columns:
                df = merge_and_rename(df, "pulocationid", "pickup_latitude", "pickup_longitude")
                
            if "dropoff_latitude" not in df.columns:
                df = merge_and_rename(df, "dolocationid", "dropoff_latitude", "dropoff_longitude")

            df = df.drop(columns=[col for col in df.columns if "Unnamed" in col], errors='ignore').dropna()

            print("Merged data from file: " + file)

            return df
        


        def process(df, file):

            """
            Green:
                Rename column: lpep_pickup_datetime, lpep_dropoff_datetime, ehail_fee
                Drop: trip_type
            Yellow:
                Rename column: tpep_pickup_datetime, tpep_dropoff_datetime, airport_fee
            """
            if file.startswith("green"):
                # rename columns
                df.rename(
                    columns={
                        "lpep_pickup_datetime": "pickup_datetime",
                        "lpep_dropoff_datetime": "dropoff_datetime",
                        "ehail_fee": "fee"
                    },
                    inplace=True
                )

                # drop column
                if "trip_type" in df.columns:
                    df.drop(columns=["trip_type"], inplace=True)

            elif file.startswith("yellow"):
                # rename columns
                df.rename(
                    columns={
                        "tpep_pickup_datetime": "pickup_datetime",
                        "tpep_dropoff_datetime": "dropoff_datetime",
                        "airport_fee": "fee"
                    },
                    inplace=True
                )

            # fix data type in columns 'payment_type', 'dolocationid', 'pulocationid', 'vendorid'
            if "payment_type" in df.columns:
                df["payment_type"] = df["payment_type"].astype(int)
            if "dolocationid" in df.columns:
                df["dolocationid"] = df["dolocationid"].astype(int)
            if "pulocationid" in df.columns:
                df["pulocationid"] = df["pulocationid"].astype(int)
            if "vendorid" in df.columns:
                df["vendorid"] = df["vendorid"].astype(int)

            # drop column 'fee'
            if "fee" in df.columns:
                df.drop(columns=["fee"], inplace=True)
                        
            # Remove missing data
            df = df.dropna()
            df = df.reindex(sorted(df.columns), axis=1)

            print("Transformed data from file: " + file)

            return df   
        def is_parquet_file(filepath):
            try:
                # Try opening with pyarrow to validate Parquet magic bytes
                pq.ParquetFile(filepath)
                return True
            except Exception:
                return False
            

        bucket_name_2 = "processed"
        self.create_bucket(bucket_name_2)
        client, _ = self.get_minio_client()

        root_folder = "data"
        for year in os.listdir(root_folder):
            full_path = os.path.join(root_folder, year)
            if os.path.isdir(full_path):
                for file in os.listdir(full_path):
                    file_path = os.path.join(full_path,file)
                    if file_path.endswith(".parquet") and is_parquet_file(file_path):
                        df = pd.read_parquet(file_path, engine='pyarrow')
                        # lower case all columns
                        df.columns = map(str.lower, df.columns)
                        df = drop_column(df, file_path)
                        df = merge_taxi_zone(df, file_path)
                        df = process(df, file_path)
                        print(df.head(3))

                        object_name = f'{year}/{file}'

                        try:
                            client.stat_object(bucket_name = bucket_name_2 , object_name = object_name)
                            print(f"-----Skip {object_name} is existed in bucket {bucket_name_2 }")
                        except Exception as e :
                            # client.fput_object(bucket_name=bucket_name_2 ,object_name=object_name, file_path=file_path)
                            # print(f"------Upload {file} to bucket {bucket_name_2 }/{object_name}")

                            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                                temp_path = tmp.name
                                df.to_parquet(temp_path, engine='pyarrow', index=False)


                                # Upload file tạm này lên MinIO
                                client.fput_object(bucket_name=bucket_name_2, object_name=object_name, file_path=temp_path)
                                print(f"------Upload {file} to bucket {bucket_name_2}/{object_name}")

                    else:
                        print(f"Skipping non-parquet or corrupted file: {file_path}")

                    print(f'----------Upload data to {bucket_name_2} successfully ----------')
    
    def delta_convert(self):
        bucket_name3 = "sandbox"
        bucket_name2 = "processed"
        self.create_bucket(bucket_name3)
        client, _ = self.get_minio_client()
        path_write = f"s3a://{bucket_name3}/"

        schema = StructType([
            StructField("congestion_surcharge", DoubleType(), True),
            StructField("dolocationid", LongType(), True),
            StructField("dropoff_latitude", DoubleType(), True),
            StructField("dropoff_longitude", DoubleType(), True),
            StructField("ehail_fee", StringType(), True), 
            StructField("extra", DoubleType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("improvement_surcharge", DoubleType(), True),
            StructField("lpep_dropoff_datetime", TimestampType(), True),
            StructField("lpep_pickup_datetime", TimestampType(), True),
            StructField("mta_tax", DoubleType(), True),
            StructField("passenger_count", DoubleType(), True),
            StructField("payment_type", LongType(), True),
            StructField("pickup_latitude", DoubleType(), True),
            StructField("pickup_longitude", DoubleType(), True),
            StructField("pulocationid", LongType(), True),
            StructField("ratecodeid", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("trip_type", IntegerType(), True),
            StructField("vendorid", LongType(), True)
        ])

        for file in client.list_objects(bucket_name2, recursive=True):
            path_read = f"s3a://{bucket_name2}/" + file.object_name
            print(f"------Reading parquet file: {file.object_name}")

            df = self.spark.read.schema(schema).parquet(path_read)

            path_write = f"s3a://{bucket_name3}/" + file.object_name
            print(f"--------Saving delta file: {file.object_name}----------")

            df.write.format("delta").mode("overwrite").save(path_write)
            print(f"---------Save delta file: {file.object_name} successfully----------") 
        
        print(f"---------Save delta bucket: {bucket_name3} successfully----------")
        



    
            
            
    

        
