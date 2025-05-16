from dotenv import load_dotenv
import os
from azure.storage.blob import ContainerClient
import json
import time

class AzureStorageContainerBlobManagement(object):

    def __init__(self, container_name: str):
        self.container_name = container_name
        load_dotenv("./../.env")
        self.url = "https://{}.blob.core.windows.net".format(os.getenv("STORAGE_ACCOUNT_NAME"))
        self.connection_string = os.getenv("STORAGE_CONNECTION_STRING")
        self.container_client = (ContainerClient.from_connection_string(self.connection_string, container_name=container_name))


    def save_to_blob(self, blob_name: str, json_record: dict[str, str]):
        json_object = json.dumps(json_record, indent=2, separators=(',', ':'), ensure_ascii=False)
        container_client = self.container_client
        source_blob_client = container_client.get_blob_client(blob_name)
        blob_info = source_blob_client.upload_blob(json_object, blob_type="BlockBlob", overwrite=True)
        print(blob_info)

    def clear_container(self):
        container_client = self.container_client
        if container_client.exists():

            container_client.delete_container()
            time.sleep(2)

            while not container_client.exists():
                time.sleep(2)
                try:
                    container_client.create_container()
                except Exception as ex:
                    print("Wait for container to be removed...")
                    template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                    message = template.format(type(ex).__name__, ex.args)
                    print(message)

        else:
            print(f"Creating new container {self.container_name}")
            container_client = self.container_client
            container_client.create_container()

        print(f"Container {self.container_name} has been recreated.")