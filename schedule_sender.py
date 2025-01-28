import os
from utils import get_tg_client, send_msg_to_myself
from hiden_utils import print_family_schedule


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
