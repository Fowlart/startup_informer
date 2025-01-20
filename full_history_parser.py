import os
from telethon import TelegramClient
from telethon.tl.patched import Message
from telethon.tl.custom.dialog import Dialog

from utils import get_tg_client

async def traverse_full_history(client: TelegramClient):

    async for d in client.iter_dialogs():
        async for m in client.iter_messages(entity=d):
            message: Message = m
            dialog: Dialog = d
            if message.from_id is None:
                line: str = f"{os.linesep}[dialog]{os.linesep}{dialog.title}{os.linesep}[date]{os.linesep}{message.date.date()}{os.linesep}[message]{os.linesep}{message.message}"
                print(line)


if __name__ =="__main__":

    print(f"Starting of script execution `{os.path.basename(__file__)}`. PID {os.getpid()}")

    client = get_tg_client()

    with client:
        client.loop.run_until_complete(traverse_full_history(client))

    print(f"Done.")