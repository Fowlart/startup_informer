import asyncio

async def count(identifier: str):
    print(f"[{identifier}] One")

    # `await` - passes function control back to the event loop
    # `asyncio.sleep` - non-blocking call:
    await asyncio.sleep(1)

    print(f"[{identifier}] Two")

async def main():
    await asyncio.gather(count("=1="), count("=2="), count("=3="))

if __name__ == "__main__":
    import time
    s = time.perf_counter()
    asyncio.run(main())
    elapsed = time.perf_counter() - s
    # will be executed in 1 second instead of 3
    print(f"Executed in {elapsed} seconds.")