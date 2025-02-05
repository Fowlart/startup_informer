import datetime
import os
import sys
from asyncio import Task

from telethon import TelegramClient
from telethon.tl.patched import Message
from telethon.tl.custom.dialog import Dialog
import asyncio
import datetime as dt
import json
from utils import get_tg_client
import shutil
import codecs
import time

async def find_messages_in_dialog(dialog: Dialog,
                                  client: TelegramClient,
                                  search_term: str):

    print(f"Processing dialog `{dialog.title}`")

    async for m in client.iter_messages(entity=dialog.entity, limit=None):

        message: Message = m

        text_message: str = message.message

        folder_name = dialog.title.replace(" ", "_").replace("/", "_")

        if text_message and ( search_term.lower() in text_message.lower() ):

            if not os.path.isdir(f"dialogs/{folder_name}"):
                os.mkdir(f"dialogs/{folder_name}")

            file_name = time.time_ns()

            with codecs.open(f"./dialogs/{folder_name}/{file_name}.json", "w","utf-8","replace") as file:
                # Data to be written
                json_record = {
                    "crawling_date": str(dt.datetime.now()),
                    "message_date": str(message.date.date()),
                    "message_text": text_message,
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

    if os.path.isdir("dialogs"):
        shutil.rmtree("dialogs")

    os.mkdir("dialogs")

    print(f"Starting of script execution `{os.path.basename(__file__)}`. PID: {os.getpid()}. Search term: `{sys.argv[1]}`")

    client = get_tg_client()

    with client:
        client.loop.run_until_complete(traverse_full_history(client, sys.argv[1]))

    print(f"Done.")