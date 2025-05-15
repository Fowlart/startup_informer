import os
import time

from azure.core.paging import ItemPaged
from azure.storage.blob import BlobProperties
from azure.storage.blob.aio import BlobPrefix
from telethon import TelegramClient
import shutil
import json
import codecs

from utilities.AzureAuth import AzureAuth

az = AzureAuth("telegram-messages")


def get_tg_client() -> TelegramClient:
    api_id = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")
    return TelegramClient("init_session", int(api_id), api_hash)

async def send_file_to_myself(client: TelegramClient, file_path: str):
    await client.send_file("me", file_path)

async def send_msg_to_myself(client: TelegramClient, msg: str):
    await client.send_message("me",msg)

async def print_dialogs(client: TelegramClient):
    async for dialog in client.iter_dialogs():
        print(dialog.name, "has id",dialog.id)

def execute_init_procedure(func, search_term: str = ""):

    client: TelegramClient = get_tg_client()

    if os.path.isdir("dialogs"):
        for root, dirs, files in os.walk("dialogs", topdown=False):
            for file in files:
                path = os.path.join(root, file)
                print(fr"removing file {path}")
                os.remove(path)
            for dir_ in dirs:
                shutil.rmtree(os.path.join(root, dir_))

    print(
        f"Starting of script execution `{os.path.basename(__file__)}`. PID: {os.getpid()}")

    with client:
        client.loop.run_until_complete(func(client, search_term))

    print(f"Done.")

def save_to_local_fs(path: str, json_record: dict[str,str]):
    with codecs.open(path, "w", "utf-8", "replace") as file:
        json_object = json.dumps(json_record, indent=2, separators=(',', ':'), ensure_ascii=False)
        file.write(json_object)
        file.close()

#todo: implement
def save_to_blob(blob_name: str, json_record: dict[str,str]):
    json_object = json.dumps(json_record, indent=2, separators=(',', ':'), ensure_ascii=False)
    az.auth_connection_string()
    container_client = az.container_client
    source_blob_client = container_client.get_blob_client(blob_name)
    blob_info = source_blob_client.upload_blob(json_object, blob_type="BlockBlob", overwrite=True)
    print(blob_info)


def clear_container(container_name: str):
    container_client = az.get_container_client(container_name)
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
        print(f"Creating new container {container_name}")
        container_client = az.get_container_client(container_name)
        container_client.create_container()

    print(f"Container {container_name} has been recreated.")
