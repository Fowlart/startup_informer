import os
import sys
from asyncio import Task
from logging import NullHandler

from telethon import TelegramClient
from telethon.tl.patched import Message
from telethon.tl.custom.dialog import Dialog
import asyncio
import datetime as dt
import json
import shutil
import codecs
import time
sys.path.append(os.path.abspath(os.path.join(".", "../startup_informer")))
print(sys.path)
from utilities.utils import get_tg_client

async def find_messages_in_dialog(dialog: Dialog,
                                  client: TelegramClient,
                                  search_term: str):

    print(f"Processing dialog `{dialog.title}`")

    async for m in client.iter_messages(entity=dialog.entity, limit=None):

        message: Message = m

        text_message: str = message.raw_text

        folder_name = dialog.title.replace(" ", "_").replace("/", "_")

        if text_message and ( search_term.lower() in text_message.lower() ):

            if not os.path.isdir(f"dialogs/{folder_name}"):
                os.mkdir(f"dialogs/{folder_name}")

            file_name = time.time_ns()
            user: str = None
            if message.sender:
                user = str(message.sender)

            with codecs.open(f"./dialogs/{folder_name}/{file_name}.json", "w","utf-8","replace") as file:
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

                json_object = json.dumps(json_record, indent=2,separators=(',', ':'),ensure_ascii=False)

                file.write(json_object)

                file.close()

    print(f"Dialog `{dialog.title}` was fully parsed.")

async def traverse_full_history(client: TelegramClient, search_term: str):
    tasks: list[Task] = []
    async for d in client.iter_dialogs():
        tasks.append(client.loop.create_task(find_messages_in_dialog(client=client, dialog=d, search_term=search_term)))

    await asyncio.gather(*tasks)

if __name__ =="__main__":

    if os.path.isdir("../dialogs"):
        shutil.rmtree("../dialogs")
    os.mkdir("../dialogs")

    print(f"Starting of script execution `{os.path.basename(__file__)}`. PID: {os.getpid()}. Search term: `{sys.argv[1]}`")

    client = get_tg_client()

    with client:
        client.loop.run_until_complete(traverse_full_history(client, sys.argv[1]))

    print(f"Done.")