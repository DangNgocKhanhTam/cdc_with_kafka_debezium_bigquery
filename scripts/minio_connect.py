
from minio import Minio
from minio.error import S3Error
class MinioConnect:
    def __init__(self, host, port, access_key, secret_key,secure =False):
        self.endpoint=f"{host}:{port}"
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self.config = {"endpoint":self.endpoint,"access_key":self.access_key,"secret_key":self.secret_key,"secure":self.secure}
        self.client = None
    
    def connect(self):
        try:

            self.client = Minio(**self.config)
            print("-----------connected to minio--------")
            return self.client
        except S3Error as e :
            print(f"---------Failed to connect Minio:{e}---------")

    def close(self):
        self.client = None

    def __enter__(self):
        self.connect()
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
