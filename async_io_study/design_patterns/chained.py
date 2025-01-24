import asyncio
import random
import time
from typing import Coroutine, Generator


async def part1(n: str) -> str:
    i = random.randint(0, 10)
    print(f"part1({n}) sleeping for {i} seconds.")
    await asyncio.sleep(i)
    result = f"part1_result[{n}]"
    print(result)
    return result

async def part2(n: str, arg: str) -> str:
    i = random.randint(0, 10)
    print(f"part2{n, arg} sleeping for {i} seconds.")
    await asyncio.sleep(i)
    result = f"part2_result[{n}] << {arg}"
    print(result)
    return result

async def chain(n: str) -> None:
    start = time.perf_counter()
    p1 = await part1(n)
    p2 = await part2(n, p1)
    end = time.perf_counter() - start
    print(f"chained_result[{n}] << {p2} (took {end:0.2f} seconds).")

async def main(*args):
    coroutines: Generator[Coroutine] = (chain(n) for n in args)
    await asyncio.gather(*coroutines)

if __name__ == "__main__":

    random.seed(444)
    args = ["first", "second", "third"]
    start = time.perf_counter()
    asyncio.run(main(*args))
    end = time.perf_counter() - start
    print(f"Program finished in {end} seconds.")