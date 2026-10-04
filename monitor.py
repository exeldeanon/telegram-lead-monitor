#!/usr/bin/env python3
"""
Telegram Lead Monitor v2
- Юзербот слушает все группы/каналы аккаунта
- Бот отправляет алерты с кнопкой "Отреагировать"
- Команды: /target /keywords /chats /stats
- Состояние хранится в state.json
"""
from __future__ import annotations
import asyncio, hashlib, html, json, logging, os, re, sys, time, unicodedata
from pathlib import Path
from typing import Optional
import aiohttp
from cachetools import TTLCache
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.tl.types import Channel, Chat as TGChat, MessageService, User

load_dotenv()
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION_NAME = os.environ.get("SESSION_NAME", "monitor_session")
BOT_TOKEN = os.environ["BOT_TOKEN"]
KEYWORDS_FILE = os.environ.get("KEYWORDS_FILE", "keywords.txt")
DEDUP_TTL = int(os.environ.get("DEDUP_TTL", 1800))
PHONE = os.environ.get("PHONE", "")
HEADLESS = os.environ.get("HEADLESS", "true").lower() == "true"
STATE_FILE = os.environ.get("STATE_FILE", "state.json")
DEFAULT_TARGET = int(os.environ.get("TARGET_CHAT_ID", "0"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout),
              logging.FileHandler("monitor.log", encoding="utf-8")],
)
logger = logging.getLogger("lead_monitor")


# ── State Manager ───────────────────────────────────────────────────────────

class State:
    """Persistent JSON state: keywords, target, stats, reactions."""

    def __init__(self, path: str):
        self.path = Path(path)
        self.data = {
            "target_chat_id": DEFAULT_TARGET,
            "keywords": [],
            "stats": {"total_triggers": 0, "reacted": 0},
            "reactions": {},  # alert_msg_id -> {"user": name, "time": ts}
        }
        self._load()

    def _load(self):
        if self.path.exists():
            try:
                saved = json.loads(self.path.read_text(encoding="utf-8"))
                self.data.update(saved)
                logger.info("State loaded from %s", self.path)
            except Exception:
                logger.warning("Corrupt state file, using defaults")

    def save(self):
        self.path.write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8")

    @property
    def target(self) -> int:
        return self.data["target_chat_id"]

    @target.setter
    def target(self, v: int):
        self.data["target_chat_id"] = v
        self.save()

    @property
    def keywords(self) -> list[str]:
        return self.data["keywords"]

    @keywords.setter
    def keywords(self, v: list[str]):
        self.data["keywords"] = v
        self.save()

    def add_reaction(self, msg_id: int, user_name: str):
        self.data["reactions"][str(msg_id)] = {"user": user_name, "time": int(time.time())}
        self.data["stats"]["reacted"] += 1
        self.save()

    def get_reaction(self, msg_id: int) -> Optional[dict]:
        return self.data["reactions"].get(str(msg_id))

    def inc_triggers(self):
        self.data["stats"]["total_triggers"] += 1
        self.save()

    @property
    def stats(self) -> dict:
        return self.data["stats"]


# ── Helpers ─────────────────────────────────────────────────────────────────

def load_lines(fp: str) -> list[str]:
    p = Path(fp)
    if not p.exists():
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


# ── Bot API Client ──────────────────────────────────────────────────────────

class BotAPI:
    BASE = "https://api.telegram.org/bot{token}/{method}"

    def __init__(self, token: str, state: State):
        self.token = token
        self.state = state
        self._session: Optional[aiohttp.ClientSession] = None
        self._offset = 0
        self._running = False
        self._chats_cache: list[str] = []

    async def _sess(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def api(self, method: str, **kw) -> dict:
        url = self.BASE.format(token=self.token, method=method)
        s = await self._sess()
        # Long polling needs timeout > server-side timeout param
        t = 65 if method == "getUpdates" else 30
        try:
            async with s.post(url, json=kw, timeout=aiohttp.ClientTimeout(total=t)) as r:
                data = await r.json()
                if not data.get("ok"):
                    logger.error("Bot API %s: %s", method, data)
                return data
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Bot API %s failed", method)
            return {}

    async def send_alert(self, chat_id: int, body: str) -> Optional[int]:
        kb = {"inline_keyboard": [[
            {"text": "\u2705 \u041e\u0442\u0440\u0435\u0430\u0433\u0438\u0440\u043e\u0432\u0430\u0442\u044c", "callback_data": "react"}
        ]]}
        resp = await self.api("sendMessage", chat_id=chat_id, text=body,
                              parse_mode="HTML", disable_web_page_preview=True,
                              reply_markup=kb)
        return resp["result"]["message_id"] if resp.get("ok") else None

    async def edit_msg(self, chat_id: int, msg_id: int, text: str, kb=None):
        kw = {"chat_id": chat_id, "message_id": msg_id, "text": text,
              "parse_mode": "HTML", "disable_web_page_preview": True}
        if kb:
            kw["reply_markup"] = kb
        await self.api("editMessageText", **kw)

    async def answer_cb(self, cb_id: str, text: str = ""):
        await self.api("answerCallbackQuery", callback_query_id=cb_id, text=text)

    async def send(self, chat_id: int, text: str):
        await self.api("sendMessage", chat_id=chat_id, text=text, parse_mode="HTML")

    async def _handle_cmd(self, msg: dict):
        text = msg.get("text", "").strip()
        # Strip @BotName suffix from commands
        if "@" in text.split()[0]:
            text = text.split("@")[0] + (" " + " ".join(text.split()[1:]) if len(text.split()) > 1 else "")
        text = text.strip()
        cid = msg["chat"]["id"]
        if text in ("/start", "/help"):
            await self.send(cid, "\U0001f916 <b>Lead Monitor</b>\n/start /target /keywords /addkeyword /delkeyword /chats /stats")
        elif text.startswith("/target"):
            parts = text.split(maxsplit=1)
            if len(parts) < 2:
                await self.send(cid, f"Target: <code>{self.state.target}</code>")
            else:
                try:
                    self.state.target = int(parts[1]); await self.send(cid, f"\u2705 <code>{parts[1]}</code>")
                except ValueError:
                    await self.send(cid, "\u274c Bad ID")
        elif text == "/keywords":
            kws = self.state.keywords
            if kws:
                await self.send(cid, "\n".join(f"\u2022 <code>{html.escape(k)}</code>" for k in kws))
            else:
                await self.send(cid, "No keywords")
        elif text.startswith("/addkeyword"):
            kw = text.split(maxsplit=1)[1].strip() if len(text.split(maxsplit=1)) > 1 else ""
            if kw:
                kws = self.state.keywords
                if normalize(kw) not in [normalize(k) for k in kws]:
                    kws.append(kw); self.state.keywords = kws
                    await self.send(cid, f"\u2705 <code>{html.escape(kw)}</code>")
                else:
                    await self.send(cid, "\u26a0\ufe0f Exists")
            else:
                await self.send(cid, "/addkeyword phrase")
        elif text.startswith("/delkeyword"):
            kw = text.split(maxsplit=1)[1].strip() if len(text.split(maxsplit=1)) > 1 else ""
            if kw:
                kws = self.state.keywords; nk = [k for k in kws if normalize(k) != normalize(kw)]
                if len(nk) < len(kws):
                    self.state.keywords = nk; await self.send(cid, f"\U0001f5d1 <code>{html.escape(kw)}</code>")
                else:
                    await self.send(cid, "\u274c Not found")
            else:
                await self.send(cid, "/delkeyword phrase")
        elif text == "/chats":
            if self._chats_cache:
                await self.send(cid, "\n".join(f"\u2022 {html.escape(c)}" for c in self._chats_cache[:50]))
            else:
                await self.send(cid, "Not loaded yet")
        elif text == "/stats":
            s = self.state.stats; t, r = s["total_triggers"], s["reacted"]
            p = round(r/t*100,1) if t else 0
            await self.send(cid, f"Triggers: {t} | Reacted: {r} ({p}%) | Pending: {t-r}")
        else:
            return
        # If we got here without sending anything for a known command, log it
    async def _handle_cmd_safe(self, msg: dict):
        try:
            await self._handle_cmd(msg)
        except Exception as e:
            logger.exception("Command handler error")
            cid = msg.get("chat", {}).get("id")
            if cid:
                await self.send(cid, f"\u274c Error: {e}")

    async def _handle_cb(self, cb: dict):
        cid = cb["message"]["chat"]["id"]
        mid = cb["message"]["message_id"]
        un = cb.get("from", {}).get("username") or cb.get("from", {}).get("first_name", "?")
        if cb.get("data") == "react":
            ex = self.state.get_reaction(mid)
            if ex:
                await self.answer_cb(cb["id"], f"Already: @{ex['user']}")
                return
            self.state.add_reaction(mid, un)
            await self.answer_cb(cb["id"], "\u2705 Done!")
            old = cb["message"].get("text", "")
            new = old + f"\n\n\u2705 <b>\u041e\u0442\u0440\u0435\u0430\u0433\u0438\u0440\u043e\u0432\u0430\u043b:</b> @{html.escape(un)}"
            kb = {"inline_keyboard": [[{"text": f"\u2705 @{un}", "callback_data": "done"}]]}
            await self.edit_msg(cid, mid, new, kb)

    async def poll(self):
        self._running = True
        logger.info("Bot polling started")
        while self._running:
            try:
                resp = await self.api("getUpdates", offset=self._offset, timeout=30)
                for upd in resp.get("result", []):
                    self._offset = upd["update_id"] + 1
                    if "message" in upd and upd["message"].get("text", "").startswith("/"):
                        await self._handle_cmd_safe(upd["message"])
                    elif "callback_query" in upd:
                        await self._handle_cb(upd["callback_query"])
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Poll error")
                await asyncio.sleep(5)

    def stop(self):
        self._running = False

    async def close(self):
        self.stop()
        if self._session and not self._session.closed:
            await self._session.close()


class LeadMonitor:
    def __init__(self):
        self.state = State(STATE_FILE)
        if not self.state.keywords:
            fk = load_lines(KEYWORDS_FILE)
            if fk:
                self.state.keywords = fk
                logger.info("Loaded %d keywords from file", len(fk))
        self.pat = build_pattern(self.state.keywords)
        self.cache: TTLCache = TTLCache(maxsize=10_000, ttl=DEDUP_TTL)
        self.bot = BotAPI(BOT_TOKEN, self.state)
        self.client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
        self.watched: set[int] = set()

    async def _start_client(self):
        sess = Path(f"{SESSION_NAME}.session").exists()
        if not sess:
            if not PHONE:
                logger.error("No session file and no PHONE env var."); sys.exit(1)
            logger.warning("No session file found, will authorize with phone %s", PHONE)
        phone = PHONE if PHONE else None
        attempts = 0
        while True:
            try:
                await self.client.start(phone=phone)
                return
            except FloodWaitError as e:
                attempts += 1
                wait = min(e.seconds + 5, 600)
                logger.warning("FloodWait %ds during auth (attempt %d), waiting %ds...", e.seconds, attempts, wait)
                if attempts > 5:
                    logger.error("Too many FloodWait errors. Session file may be missing/corrupt on server.")
                    logger.error("Upload a valid %s.session file or set PHONE env var.", SESSION_NAME)
                    sys.exit(1)
                await asyncio.sleep(wait)
            except Exception as e:
                err = str(e).lower()
                if "phone number" in err or "invalid" in err:
                    logger.error("Auth failed: %s. Check PHONE env var.", e)
                    sys.exit(1)
                raise

    async def discover_chats(self):
        me = await self.client.get_me(); count = 0; names = []
        async for d in self.client.iter_dialogs():
            if d.is_user: continue
            e = d.entity
            if getattr(e,'id',None)==me.id: continue
            if isinstance(e,(Channel,TGChat)):
                self.watched.add(e.id)
                t=getattr(e,'title','?'); u=getattr(e,'username',None)
                names.append(f"{t} (@{u})" if u else t); count+=1
        self.bot._chats_cache=names
        if not self.watched:
            logger.error("No chats!"); await self.client.disconnect(); sys.exit(1)
        logger.info("Discovered %d chats",count)

    def is_dup(self,c,t):
        k=dedup_key(c,t)
        if k in self.cache: return True
        self.cache[k]=True; return False

    def find_trig(self,nt):
        m=self.pat.search(nt); return m.group(0) if m else None

    async def on_msg(self, ev):
        try:
            if ev.out or isinstance(ev.message, MessageService): return
            sender = await ev.get_sender()
            if isinstance(sender, User) and sender.bot: return
            if ev.chat_id not in self.watched: return
            raw = ev.raw_text or ""
            if not raw.strip(): return
            self.pat = build_pattern(self.state.keywords)
            trig = self.find_trig(normalize(raw))
            if trig is None: return
            if self.is_dup(ev.chat_id, raw): return
            chat = await ev.get_chat()
            ct = getattr(chat,"title",None) or getattr(chat,"first_name","?")
            cu = getattr(chat,"username",None)
            if isinstance(sender,User):
                sn=f"{sender.first_name or ''} {sender.last_name or ''}".strip() or "?"
                su,sid=sender.username,sender.id
            else:
                sn,su,sid=str(sender) if sender else "?",None,0
            lnk=msg_link(chat,ev.id)
            logger.info("LEAD|%s|'%s'|%s",ct,trig,sn)
            cte=html.escape(ct)
            if cu: cte+=f" (@{html.escape(cu)})"
            up=f"@{html.escape(su)} / " if su else ""
            body=(f"\U0001f6a8 <b>\u041b\u0438\u0434!</b>\n\n\U0001f4cd {cte}\n"
                  f"\U0001f464 {html.escape(sn)} ({up}ID:<code>{sid}</code>)\n"
                  f"\U0001f511 <code>{html.escape(trig)}</code>\n\n"
                  f"<blockquote>{html.escape(raw)[:3900]}</blockquote>\n\n"
                  f'<a href="{lnk}">\u041f\u0435\u0440\u0435\u0439\u0442\u0438</a>')
            t=self.state.target
            if t:
                mid=await self.bot.send_alert(t,body)
                if mid: self.state.inc_triggers()
            else:
                logger.warning("No target! Use /target")
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds)
        except ConnectionError:
            logger.error("Disconnected")
        except Exception:
            logger.exception("Error")

    async def run(self):
        logger.info("Starting v2...")
        await self._start_client()
        await self.discover_chats()
        self.client.add_event_handler(self.on_msg, events.NewMessage(incoming=True))
        logger.info("Listening %d chats, target=%s",len(self.watched),self.state.target)
        pt=asyncio.create_task(self.bot.poll())
        try:
            await self.client.run_until_disconnected()
        except KeyboardInterrupt:
            pass
        finally:
            self.bot.stop(); pt.cancel(); await self.bot.close()


if __name__=="__main__":
    try: asyncio.run(LeadMonitor().run())
    except KeyboardInterrupt: logger.info("Stopped")
