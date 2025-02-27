import asyncio
import itertools as it
import os
import random
import time
from asyncio import Task


async def make_item(size: int = 5) -> str:
    return os.urandom(size).hex()



async def rand_sleep(caller=None) -> None:
    i = random.randint(0, 10)
    if caller:
        print(f"{caller} sleeping for {i} seconds.")
    await asyncio.sleep(i)



async def produce(name: int, q: asyncio.Queue) -> str:
    n = random.randint(0, 10)
    for _ in it.repeat(None, n):  # Synchronous loop for each single producer
        await rand_sleep(caller=f"Producer {name}")
        i =  await make_item()
        t = time.perf_counter()

        # the main action in `produce`
        await q.put((i, t))
        print(f"Producer {name} added <{i}> to queue.")

    return f"Producer {name} finished!"



async def consume(name: int, q: asyncio.Queue) -> None:
    print(f"Consumer {name} has been started!")
    while True:
        await rand_sleep(caller=f"Consumer {name}")
        i, t = await q.get()
        now = time.perf_counter()
        print(f"Consumer {name} got element <{i}> in {now-t:0.5f} seconds.")
        q.task_done()



async def main(nprod: int, ncon: int):

    q = asyncio.Queue()

    # this will start producers immediately
    producers: list[Task] = [asyncio.create_task(produce(n, q)) for n in range(nprod)]

    # this will start consumers immediately
    consumers: list[Task]  = [asyncio.create_task(consume(n, q)) for n in range(ncon)]

    # this will await until all producers produced
    results = await asyncio.gather(*producers)
    print("All producers have produced to the queue: ")
    for r in results:
        print(r)

    # Block until all items in the queue have been gotten and processed.
    await q.join()

    # this will cancel consumers immediately
    for c in consumers:
        print(f"Stopping consumer {c}")
        c.cancel()



if __name__ == "__main__":
    random.seed(444)
    start = time.perf_counter()
    asyncio.run(main(nprod=3, ncon=10))
    elapsed = time.perf_counter() - start
    print(f"Program completed in {elapsed:0.5f} seconds.")