#!/usr/bin/env python3
"""Создание session файла для Telethon"""
import asyncio, os
from dotenv import load_dotenv
load_dotenv()

from telethon import TelegramClient

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
PHONE = os.environ["PHONE"]
SESSION = os.environ.get("SESSION_NAME", "monitor_session")

async def main():
    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.start(phone=PHONE)
    me = await client.get_me()
    print(f"\n✅ Session created! User: {me.first_name} (@{me.username or 'N/A'}, ID: {me.id})")
    print(f"   File: {SESSION}.session")
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
