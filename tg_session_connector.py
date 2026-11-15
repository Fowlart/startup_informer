import asyncio
import os
from pathlib import Path
import qrcode
from dotenv import load_dotenv
from telethon import TelegramClient

env_path = Path.home() / ".secrets" / "startup_informer.env"
load_dotenv(env_path, override=True, encoding="utf-8-sig")

client = TelegramClient(
    str(Path.home() / ".secrets" / "init_session"),
    int(os.getenv("TELEGRAM_API_ID")),
    os.getenv("TELEGRAM_API_HASH"),
    device_model="ASUS Zenbook 14",
    system_version="Windows 11",
    app_version="1.42.0",
)


async def qr_auth() -> None:
    try:
        await client.connect()
        if await client.is_user_authorized():
            print("Сесія вже активна!")
            return
        qr_login = await client.qr_login()
        qr = qrcode.QRCode(border=1)
        qr.add_data(qr_login.url)
        qr.print_ascii(invert=True)
        print("Відскануй QR-код у Telegram: Налаштування → Пристрої → Підключити пристрій")
        await qr_login.wait(timeout=60)
        me = await client.get_me()
        print(f"Успішно авторизовано: {me.first_name} (@{me.username})")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(qr_auth())