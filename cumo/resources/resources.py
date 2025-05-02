from io import BytesIO, StringIO
from dagster import ConfigurableResource
import requests
from requests.auth import HTTPBasicAuth
import os
from minio import Minio
from dagster_factory_pipelines import registry

@registry.register_resource("minio")    
class MinioBucket(ConfigurableResource): # can be replaces with AWS3 resource
    access_key:str
    secret_key:str
    host:str

    def create_client(self):
        return Minio(
            self.host,
            # allow only env variables, because dagster exposes credentials in UI
            access_key=os.getenv(self.access_key),
            secret_key=os.getenv(self.secret_key),
            secure=False
        )
    
    def create_bucket_if_not_exists(self, client, bucket_name):
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")

    def upload_obj(self, bucket_name, file_name, obj) -> None:
        client = self.create_client()
        self.create_bucket_if_not_exists(client, bucket_name)
        if isinstance(obj, StringIO):
            encoded_content = obj.getvalue().encode('utf-8') 
            buffer = BytesIO(encoded_content)
            buffer_len = len(encoded_content)
        else:
            buffer = BytesIO(obj)
            buffer_len = len(obj)

        buffer.seek(0)

        client.put_object(
            bucket_name,
            file_name,
            data=buffer,
            length=buffer_len
)

    def get_obj(self, bucket_name, obj_name) -> BytesIO:
        client = self.create_client()
        try:
            response = client.get_object(bucket_name, obj_name)
            obj= BytesIO(response.data)
            
        finally:
            response.close()
            response.release_conn()
        return obj