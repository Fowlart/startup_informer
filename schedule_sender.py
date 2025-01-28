import os
from telethon import TelegramClient
from telethon.tl.patched import Message
import datetime
from utils import get_tg_client, send_msg_to_myself

async def print_family_schedule(client: TelegramClient):
        string_result = []
        wife: int = 553068238
        key_phrases = ["графік", "на", ":"]
        boundary_date_to_consider_messages = (datetime.datetime.now() - datetime.timedelta(days=3)).date()

        async for m in client.iter_messages(entity=wife):
            ms: Message = m

            if (ms.message
                and all( (phrase in  ms.message.lower()) is True for phrase in key_phrases)
                and ms.date.date() > boundary_date_to_consider_messages
                and ms.sender_id == wife
            ):

                string_result.append(os.linesep)
                string_result.append(str(ms.date))
                string_result.append(ms.message)
                print(f"{ms.message}")

                await send_msg_to_myself(client=client, msg=os.linesep.join(string_result))

                string_result.clear()


if __name__=="__main__":

    print(f"{os.linesep*2}Starting of script execution `{os.path.basename(__file__)}`. PID {os.getpid()}")

    with open("./startup_info") as f:
        lines = [x.strip() for x in f.readlines()]

    index_of_separator = lines.index("Connected wireless networks, in a recent 2 days")

    msg=os.linesep.join(lines[index_of_separator:])

    client = get_tg_client()

    with client:
        client.loop.run_until_complete(send_msg_to_myself(client, msg))
        client.loop.run_until_complete(print_family_schedule(client))

    print("Done.")
