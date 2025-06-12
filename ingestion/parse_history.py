import os
import sys
from asyncio import Task
from typing import Any

from telethon import TelegramClient
from telethon.tl.patched import Message
from telethon.tl.custom.dialog import Dialog
import asyncio
import datetime as dt
import time
sys.path.append(os.path.abspath(os.path.join(".", "../startup_informer")))
print(sys.path)
from utilities.utils import execute_init_procedure, save_to_local_fs
from dotenv import load_dotenv


async def find_and_ingest_messages(dialog: Dialog,
                                   client: TelegramClient,
                                   search_term: str):
    load_dotenv("./../.env")

    print(f"Processing dialog `{dialog.title}`")

    async for m in client.iter_messages(entity=dialog.entity, limit=None):

        message: Message = m

        text_message: str = message.raw_text

        folder_name = (dialog
                       .title
                       .replace(" ", "_")
                       .replace("/", "_")
                        .replace("|","_")
                       .replace(":",""))


        if text_message and ( search_term.lower() in text_message.lower() ):

            if not os.path.isdir(f"dialogs/{folder_name}"):
                os.mkdir(f"dialogs/{folder_name}")

            file_name = time.time_ns()
            file_name = str(file_name) + ".json"

            try:
                sender_dict: dict[Any, Any] = message.sender.to_dict()
                user = {str(k): str(v) for k, v in sender_dict.items()}
            except Exception as e:
                print(f"Error processing sender: {e}")
                user = "no info"

                # Data to be written
            json_record = {
                    "crawling_date": str(dt.datetime.now()),
                    "message_date": str(message.date.date()),
                    "message_text": text_message,
                    "dialog": dialog.title,
                    "post_author": message.post_author,
                    "is_channel": message.is_channel,
                    "is_group": message.is_group,
                    "user": user
            }

            save_to_local_fs(path=f"./dialogs/{folder_name}/{file_name}", json_record=json_record)


    print(f"Dialog `{dialog.title}` was fully parsed.")

async def traverse_full_history(client: TelegramClient, search_term: str):

    tasks: list[Task] = []

    async for d in client.iter_dialogs():
        tasks.append(client.loop.create_task(find_and_ingest_messages(client=client, dialog=d, search_term=search_term)))

    await asyncio.gather(*tasks)

if __name__ =="__main__":

    execute_init_procedure(func=traverse_full_history, search_term=sys.argv[1])