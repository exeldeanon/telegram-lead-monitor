#!/usr/bin/env python3
"""
Telegram Lead Monitor
Юзербот (Telethon) слушает группы из chats.txt.
При совпадении — HTML-алерт через Bot API в TARGET_CHAT_ID со ссылкой на сообщение.
"""
from __future__ import annotations
import asyncio, hashlib, html, logging, os, re, sys, unicodedata
from pathlib import Path
from typing import Optional
import aiohttp
from cachetools import TTLCache
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.tl.types import Channel, Chat, MessageService, User

load_dotenv()
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION_NAME = os.environ.get("SESSION_NAME", "monitor_session")
TARGET_CHAT_ID = int(os.environ["TARGET_CHAT_ID"])
BOT_TOKEN = os.environ["BOT_TOKEN"]
KEYWORDS_FILE = os.environ.get("KEYWORDS_FILE", "keywords.txt")
CHATS_FILE = os.environ.get("CHATS_FILE", "chats.txt")
DEDUP_TTL = int(os.environ.get("DEDUP_TTL", 1800))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout),
              logging.FileHandler("monitor.log", encoding="utf-8")],
)
logger = logging.getLogger("lead_monitor")


def load_lines(fp: str) -> list[str]:
    p = Path(fp)
    if not p.exists():
        logger.warning("File %s not found", fp)
        return []
    return [l.strip() for l in p.read_text(encoding="utf-8").splitlines()
            if l.strip() and not l.strip().startswith("#")]


def normalize(text: str) -> str:
    if not text:
        return ""
    t = unicodedata.normalize("NFKC", text).lower().replace("\u0451", "\u0435")
    t = re.sub(r"[^\w\s]", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def build_pattern(kws: list[str]) -> re.Pattern:
    kws = sorted(kws, key=len, reverse=True)
    return re.compile("|".join(re.escape(k) for k in kws), re.IGNORECASE)


def dedup_key(cid: int, text: str) -> str:
    return hashlib.sha256(f"{cid}:{normalize(text)}".encode()).hexdigest()[:16]


def msg_link(chat, mid: int) -> str:
    if isinstance(chat, Channel) and chat.username:
        return f"https://t.me/{chat.username}/{mid}"
    return f"https://t.me/c/{chat.id}/{mid}"


def fmt_alert(ct, cu, sn, su, sid, trig, txt, lnk) -> str:
    ct_e = html.escape(ct)
    if cu:
        ct_e += f" (@{html.escape(cu)})"
    up = f"@{html.escape(su)} / " if su else ""
    sd = f"{html.escape(sn)} ({up}ID: <code>{sid}</code>)"
    return (
        "\U0001f6a8 <b>\u041d\u0430\u0439\u0434\u0435\u043d \u043f\u043e\u0442\u0435\u043d\u0446\u0438\u0430\u043b\u044c\u043d\u044b\u0439 \u043b\u0438\u0434!</b>\n\n"
        f"\U0001f4cd <b>\u0427\u0430\u0442:</b> {ct_e}\n"
        f"\U0001f464 <b>\u041e\u0442\u043f\u0440\u0430\u0432\u0438\u0442\u0435\u043b\u044c:</b> {sd}\n"
        f"\U0001f511 <b>\u0422\u0440\u0438\u0433\u0433\u0435\u0440:</b> <code>{html.escape(trig)}</code>\n\n"
        f"\U0001f4ac <b>\u0422\u0435\u043a\u0441\u0442 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u044f:</b>\n"
        f"<blockquote>{html.escape(txt)[:3900]}</blockquote>\n\n"
        f'\U0001f517 <a href="{lnk}">\u041f\u0435\u0440\u0435\u0439\u0442\u0438 \u043a \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u044e</a>'
    )


class BotSender:
    URL = "https://api.telegram.org/bot{token}/sendMessage"

    def __init__(self, token: str):
        self._url = self.URL.format(token=token)
        self._sess: Optional[aiohttp.ClientSession] = None

    async def _get_sess(self) -> aiohttp.ClientSession:
        if self._sess is None or self._sess.closed:
            self._sess = aiohttp.ClientSession()
        return self._sess

    async def send(self, chat_id: int, body: str) -> bool:
        payload = {"chat_id": chat_id, "text": body,
                   "parse_mode": "HTML", "disable_web_page_preview": True}
        s = await self._get_sess()
        try:
            async with s.post(self._url, json=payload,
                              timeout=aiohttp.ClientTimeout(total=15)) as r:
                data = await r.json()
                if r.status == 200:
                    logger.info("Alert sent to %s", chat_id)
                    return True
                if r.status == 429:
                    w = data.get("parameters", {}).get("retry_after", 30)
                    logger.warning("Bot FloodWait %ds", w)
                    await asyncio.sleep(w)
                    return await self.send(chat_id, body)
                logger.error("Bot API %d: %s", r.status, data)
                return False
        except Exception:
            logger.exception("Bot send failed")
            return False

    async def close(self):
        if self._sess and not self._sess.closed:
            await self._sess.close()


class LeadMonitor:
    def __init__(self):
        raw = load_lines(KEYWORDS_FILE)
        if not raw:
            logger.error("No keywords in %s", KEYWORDS_FILE)
            sys.exit(1)
        self.kw_norm = [normalize(k) for k in raw]
        self.pat = build_pattern(self.kw_norm)
        logger.info("Loaded %d keywords", len(self.kw_norm))

        self.entries = load_lines(CHATS_FILE)
        logger.info("Loaded %d chat entries", len(self.entries))

        self.cache: TTLCache = TTLCache(maxsize=10_000, ttl=DEDUP_TTL)
        self.bot = BotSender(BOT_TOKEN)
        self.client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
        self.watched: set[int] = set()

    async def resolve(self):
        for e in self.entries:
            try:
                ent = await self.client.get_entity(e)
                self.watched.add(ent.id)
                n = getattr(ent, "title", None) or getattr(ent, "first_name", e)
                logger.info("Watching: %s (%s)", n, ent.id)
            except Exception:
                logger.warning("Cannot resolve: %s", e, exc_info=True)
        if not self.watched:
            logger.error("No chats resolved!")
            await self.client.disconnect()
            sys.exit(1)

    def is_dup(self, cid: int, txt: str) -> bool:
        k = dedup_key(cid, txt)
        if k in self.cache:
            return True
        self.cache[k] = True
        return False

    def find_trig(self, nt: str) -> Optional[str]:
        m = self.pat.search(nt)
        return m.group(0) if m else None

    async def on_msg(self, ev: events.NewMessage.Event):
        try:
            if ev.out or isinstance(ev.message, MessageService):
                return
            sender = await ev.get_sender()
            if isinstance(sender, User) and sender.bot:
                return
            if ev.chat_id not in self.watched:
                return
            raw = ev.raw_text or ""
            if not raw.strip():
                return
            trig = self.find_trig(normalize(raw))
            if trig is None:
                return
            if self.is_dup(ev.chat_id, raw):
                return

            chat = await ev.get_chat()
            ct = getattr(chat, "title", None) or getattr(chat, "first_name", "?")
            cu = getattr(chat, "username", None)
            if isinstance(sender, User):
                sn = f"{sender.first_name or ''} {sender.last_name or ''}".strip() or "?"
                su, sid = sender.username, sender.id
            else:
                sn, su, sid = str(sender) if sender else "?", None, 0

            lnk = msg_link(chat, ev.id)
            logger.info("LEAD | %s | '%s' | %s", ct, trig, sn)
            body = fmt_alert(ct, cu, sn, su, sid, trig, raw, lnk)
            await self.bot.send(TARGET_CHAT_ID, body)

        except FloodWaitError as e:
            logger.warning("FloodWait %ds", e.seconds)
            await asyncio.sleep(e.seconds)
        except ConnectionError:
            logger.error("Connection lost, reconnecting...")
        except Exception:
            logger.exception("Handler error")

    async def run(self):
        logger.info("Starting Lead Monitor...")
        await self.client.start()
        logger.info("Authorized")
        await self.resolve()
        self.client.add_event_handler(self.on_msg, events.NewMessage(incoming=True))
        logger.info("Listening %d chats → alerts to %s", len(self.watched), TARGET_CHAT_ID)
        try:
            await self.client.run_until_disconnected()
        except KeyboardInterrupt:
            logger.info("Shutdown")
        finally:
            await self.bot.close()


if __name__ == "__main__":
    m = LeadMonitor()
    try:
        asyncio.run(m.run())
    except KeyboardInterrupt:
        logger.info("Stopped")


