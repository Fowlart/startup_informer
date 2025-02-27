import os

from telethon import TelegramClient

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