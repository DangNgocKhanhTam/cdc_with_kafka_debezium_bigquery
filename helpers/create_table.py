import os
from dotenv import load_dotenv
from config.minio_config import get_postgres_config
from scripts.postgres_connect import PostgresConnect
load_dotenv(".env")

def main(config):

    with PostgresConnect(config["postgres"].host, config["postgres"].port, config["postgres"].user_name, config["postgres"].password, config["postgres"].db) as postgres_client:
        connection, cursor = postgres_client.connection, postgres_client.cursor
        create_table_iot = """
            CREATE TABLE IF NOT EXISTS iot.taxi_nyc_time_series (
                vendorid                BIGINT,
                lpep_pickup_datetime    TIMESTAMP WITHOUT TIME ZONE,
                lpep_dropoff_datetime   TIMESTAMP WITHOUT TIME ZONE,
                passenger_count         DOUBLE PRECISION,
                trip_distance           DOUBLE PRECISION,
                ratecodeid              DOUBLE PRECISION,
                store_and_fwd_flag      VARCHAR,
                pulocationid            BIGINT,
                dolocationid            BIGINT,
                payment_type            BIGINT,
                fare_amount             DOUBLE PRECISION,
                extra                   DOUBLE PRECISION,
                mta_tax                 DOUBLE PRECISION,
                tip_amount              DOUBLE PRECISION,
                tolls_amount            DOUBLE PRECISION,
                improvement_surcharge   DOUBLE PRECISION,
                total_amount            DOUBLE PRECISION,
                congestion_surcharge    DOUBLE PRECISION,
                dropoff_latitude        DOUBLE PRECISION,
                dropoff_longitude       DOUBLE PRECISION,
                pickup_latitude         DOUBLE PRECISION,
                pickup_longitude        DOUBLE PRECISION,
                trip_type               INTEGER,
                ehail_fee               VARCHAR
            );
        """

        create_table_staging = """
            CREATE TABLE IF NOT EXISTS staging.nyc_taxi (
                year                    VARCHAR,
                month                   VARCHAR,
                dow                     VARCHAR,
                vendorid                BIGINT,
                ratecodeid              DOUBLE PRECISION,
                pulocationid            BIGINT,
                dolocationid            BIGINT,
                payment_type            BIGINT,
                trip_type               INTEGER,
                lpep_pickup_datetime    TIMESTAMP WITHOUT TIME ZONE,
                lpep_dropoff_datetime   TIMESTAMP WITHOUT TIME ZONE,
                pickup_latitude         DOUBLE PRECISION,
                pickup_longitude        DOUBLE PRECISION,
                dropoff_latitude        DOUBLE PRECISION,
                dropoff_longitude       DOUBLE PRECISION,
                passenger_count         DOUBLE PRECISION,
                trip_distance           DOUBLE PRECISION,
                extra                   DOUBLE PRECISION,
                mta_tax                 DOUBLE PRECISION,
                fare_amount             DOUBLE PRECISION,
                tip_amount              DOUBLE PRECISION,
                tolls_amount            DOUBLE PRECISION,
                total_amount            DOUBLE PRECISION,
                improvement_surcharge   DOUBLE PRECISION,
                congestion_surcharge    DOUBLE PRECISION,
                ehail_fee               VARCHAR
            );
        """
        try:
            cursor.execute(create_table_iot)
            connection.commit()
            print("------create successfully table_iot --------")
            cursor.execute(create_table_staging)
            connection.commit()
            print("------create successfully table_staging --------")
        except Exception as e:
            print(f"Failed to create table with error: {e}")


if __name__ == "__main__":
    config = get_postgres_config()
    main(config)