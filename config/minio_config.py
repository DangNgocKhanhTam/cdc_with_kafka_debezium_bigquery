from dotenv import load_dotenv
import os 
from dataclasses import dataclass

@dataclass
class MinioConfig():
    host:str
    port:int
    access_key:str
    secret_key:str
    endpoint:str
    secure:bool = False

@dataclass 
class PostgresConfig():
    host:str
    port:int
    db:str
    user_name:str
    password:str


def get_minio_config():
    dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.env"))
    load_dotenv(dotenv_path=dotenv_path)
    config={
        "minio":MinioConfig(
            host=os.getenv("MINIO_HOST"), 
            port=os.getenv("MINIO_PORT"),
            endpoint = f"{os.getenv('MINIO_HOST')}:{os.getenv('MINIO_PORT')}",
            access_key=os.getenv("MINIO_ACCESS_KEY"),
            secret_key=os.getenv("MINIO_SECRET_KEY")

        )

    }
    return config


def get_postgres_config():
    dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.env"))
    load_dotenv(dotenv_path=dotenv_path)
    config={
        "postgres":PostgresConfig(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            db=os.getenv("POSTGRES_DB"),
            user_name=os.getenv("POSTGRE_USER"),
            password=os.getenv("POSTGRES_PASSWORD")
        )

    }
    return config

if __name__ == "__main__":
    minio_config = get_minio_config()
    print(minio_config)