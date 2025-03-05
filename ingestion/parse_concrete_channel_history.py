import os
import sys
from asyncio import Task
from telethon import TelegramClient
from telethon.tl.custom.dialog import Dialog
import asyncio

sys.path.append(os.path.abspath(os.path.join(".", "../startup_informer")))
print(sys.path)
from parse_history import find_and_ingest_messages
from utilities.utils import execute_init_procedure

async def traverse_full_history(client: TelegramClient, search_term: str = ""):
    tasks: list[Task] = []
    channel_to_parse = os.getenv("SPECIFIC_DIALOG")

    async for d in client.iter_dialogs():
        dialog_casted: Dialog = d
        if dialog_casted.title.lower() == channel_to_parse.lower():
            tasks.append(client.loop.create_task(find_and_ingest_messages(client=client,
                                                                          dialog=d,
                                                                          search_term=search_term)))
    await asyncio.gather(*tasks)


if __name__ =="__main__":

    execute_init_procedure(traverse_full_history)