import os
from telethon import TelegramClient
from telethon.tl.patched import Message

from utils import get_tg_client


async def traverse_full_history(client: TelegramClient):
    wife: int = 553068238

    async for m in client.iter_messages(entity=wife):
        ms: Message = m

        if ms.from_id is None:
            line: str = f"{os.linesep*2} {ms.date.date()}: {os.linesep} {ms.message}"
            print(line)


if __name__ =="__main__":

    print(f"{os.linesep*2}Starting of script execution `{os.path.basename(__file__)}`. PID {os.getpid()}")

    client = get_tg_client()

    with client:
        client.loop.run_until_complete(traverse_full_history(client))

    print("Done.")