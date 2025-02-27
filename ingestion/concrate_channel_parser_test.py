import os
import sys
from asyncio import Task
from telethon import TelegramClient
from telethon.tl.custom.dialog import Dialog
import asyncio
import shutil
sys.path.append(os.path.abspath(os.path.join(".", "../startup_informer")))
print(sys.path)
from utilities.utils import get_tg_client
from full_history_parser import find_and_ingest_messages

async def traverse_full_history(client: TelegramClient, search_term: str):
    tasks: list[Task] = []
    async for d in client.iter_dialogs():
        dial: Dialog = d
        if dial.title == "Olena":
            tasks.append(client.loop.create_task(find_and_ingest_messages(client=client, dialog=d, search_term=search_term)))
    await asyncio.gather(*tasks)

if __name__ =="__main__":

    if os.path.isdir("./dialogs"):
        shutil.rmtree("./dialogs")
    os.mkdir("./dialogs")

    print(f"Starting of script execution `{os.path.basename(__file__)}`. PID: {os.getpid()}.")

    client = get_tg_client()

    with client:
        client.loop.run_until_complete(traverse_full_history(client=client, search_term=""))

    print(f"Done.")