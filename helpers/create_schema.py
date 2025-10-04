import os
from dotenv import load_dotenv
from config.minio_config import get_postgres_config
from scripts.postgres_connect import PostgresConnect
load_dotenv(".env")


def main(config):

    with PostgresConnect(config["postgres"].host, config["postgres"].port, config["postgres"].user_name, config["postgres"].password, config["postgres"].db) as postgres_client:
        connection, cursor = postgres_client.connection, postgres_client.cursor

        create_iot_schema = """CREATE SCHEMA IF NOT EXISTS iot;"""

        create_staging_schema = """CREATE SCHEMA IF NOT EXISTS staging;"""

        create_production_schema = """CREATE SCHEMA IF NOT EXISTS production;"""

        try:
            cursor.execute(create_iot_schema)
            connection.commit()
            print("------create successfully iot schema--------")
            cursor.execute(create_staging_schema)
            connection.commit()
            print("------create successfully staging schema--------")
            cursor.execute(create_production_schema)
            connection.commit()
            print("------create successfully production schema--------")
        except Exception as e:
            print(f"Failed to create schema with error: {e}")


if __name__ == "__main__":
    config = get_postgres_config()
    main(config)