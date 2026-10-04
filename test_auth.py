#!/usr/bin/env python3
"""Test auth script - run locally to create session file."""
import asyncio, os, sys
from dotenv import load_dotenv
from telethon import TelegramClient

load_dotenv()
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
PHONE = os.environ.get("PHONE", "")
SESSION = os.environ.get("SESSION_NAME", "monitor_session")

async def main():
    if not PHONE:
        print("ERROR: Set PHONE in .env"); sys.exit(1)
    print(f"Testing auth for {PHONE}...")
    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.start(phone=PHONE, force_sms=True)
    me = await client.get_me()
    print(f"SUCCESS! Logged in as {me.first_name} (ID: {me.id})")
    print(f"Session saved to {SESSION}.session")
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
