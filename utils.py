from telethon import TelegramClient
import keyring as kr

def define_keyring_backend():
    key_ring = kr.get_keyring()
    key_ring_name = str(key_ring).split(" ")[0]
    print(f"Setting keyring with the name: {key_ring_name}")
    kr.core.set_keyring(kr.core.load_keyring(key_ring_name))
    pass

def get_tg_client() -> TelegramClient:
    define_keyring_backend()
    api_id = kr.get_password("telegram","api_id")
    api_hash = kr.get_password("telegram","api_hash")
    return TelegramClient("init_session", int(api_id), api_hash)

async def send_file_to_myself(client: TelegramClient, file_path: str):
    await client.send_file("me", file_path)

async def send_msg_to_myself(client: TelegramClient, msg: str):
    await client.send_message("me",msg)

async def print_dialogs(client: TelegramClient):
    async for dialog in client.iter_dialogs():
        print(dialog.name, "has id",dialog.id)

if __name__=="__main__":
    define_keyring_backend()