"""Generate a Telethon StringSession locally for VPS/Bothost deployment."""

from telethon.sync import TelegramClient
from telethon.sessions import StringSession


api_id = int(input("API ID: ").strip())
api_hash = input("API Hash: ").strip()

with TelegramClient(StringSession(), api_id, api_hash) as client:
    print("\nSESSION_STRING (copy it to the hosting environment):\n")
    print(client.session.save())
