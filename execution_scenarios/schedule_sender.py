import os
import sys
sys.path.append(os.path.abspath(os.path.join(".", "../startup_informer")))
from utilities.utils import get_tg_client, send_msg_to_myself
from telethon import TelegramClient
import asyncio
from telethon.tl.patched import Message
import datetime
import random

def __get_schedule_source():
    return 553068238

async def send_msg(client: TelegramClient, ms: Message):
    string_result = [os.linesep, str(ms.date), ms.message]
    print(f"Sending message with date: {ms.date}")
    await client.send_message("me", os.linesep.join(string_result))

    # code below will make the async nature of the method more visible since the actual telegram
    # API calls happen nearly at the same time
    sleep = random.randrange(8)
    print(f"sleeping for {sleep} seconds")
    await asyncio.sleep(sleep)
    print(f"Done 4 msg: {ms.date}")

async def print_family_schedule(client: TelegramClient):

    key_phrases = ["графік", "на", ":"]
    boundary_date_to_consider_messages = (datetime.datetime.now() - datetime.timedelta(days=3)).date()

    tasks = []

    async for m in client.iter_messages(entity=__get_schedule_source()):

        ms: Message = m

        print(f"Checking message with the date {ms.date}")

        if (ms.message
                and all((phrase in ms.message.lower()) is True for phrase in key_phrases)
                and ms.date.date() > boundary_date_to_consider_messages
                and ms.sender_id == __get_schedule_source()):

            client.loop.create_task(send_msg(client=client, ms=ms))
           # tasks.append(client.loop.create_task(send_msg(client=client,ms=ms)))
    # await asyncio.gather(*tasks)


if __name__=="__main__":

    print(f"{os.linesep*2}Starting of script execution `{os.path.basename(__file__)}`. PID {os.getpid()}")

    with open("startup_info") as f:
        lines = [x.strip() for x in f.readlines()]

    index_of_separator = lines.index("Connected wireless networks, in a recent 2 days")

    msg=os.linesep.join(lines[index_of_separator:])

    client = get_tg_client()

    with client:
        client.loop.run_until_complete(send_msg_to_myself(client, msg))
        client.loop.run_until_complete(print_family_schedule(client))
    print("Done.")
