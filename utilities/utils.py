import os

from telethon import TelegramClient
import shutil
import json
import codecs

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
