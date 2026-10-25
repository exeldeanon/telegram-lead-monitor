#!/usr/bin/env python3
"""
Telegram Lead Monitor v3 — Multi-Account
- Каждый аккаунт: свой Telethon-клиент, свой бот, свои keywords, свой target
- Конфигурация в config.json (массив accounts)
- Все аккаунты работают параллельно в одном процессе
"""
from __future__ import annotations
import asyncio, hashlib, hmac, html, json, logging, os, re, sys, threading, time, unicodedata
from pathlib import Path
from typing import Optional
import aiohttp
from aiohttp import web
from cachetools import TTLCache
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession
from telethon.tl.types import Channel, Chat as TGChat, MessageService, User

load_dotenv()

# ── Config loader ────────────────────────────────────────────────────────────

CONFIG_FILE = os.environ.get("CONFIG_FILE", "config.json")


def runtime_web_host() -> str:
    return os.environ.get("WEBAPP_HOST", "0.0.0.0" if os.environ.get("PORT") else "127.0.0.1")


def runtime_web_port() -> int:
    return int(os.environ.get("PORT", os.environ.get("WEBAPP_PORT", "8080")))


def load_config() -> dict:
    """Load config from config.json, with fallback to .env for single-account compat."""
    cfg_path = Path(CONFIG_FILE)
    if cfg_path.exists():
        try:
            cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
            cfg.setdefault("registry_file", os.environ.get("REGISTRY_FILE", "lead_registry.json"))
            cfg.setdefault("session_string", os.environ.get("SESSION_STRING", ""))
            cfg.setdefault("outreach_template", os.environ.get(
                "OUTREACH_TEMPLATE",
                "Здравствуйте, {first_name}! Вы писали про «{trigger}» в чате «{chat_title}». "
                "Если вопрос ещё актуален, могу предложить вариант решения. Если сообщения не нужны, напишите «стоп»."
            ))
            cfg.setdefault("operator_user_ids", [int(x) for x in os.environ.get("OPERATOR_USER_IDS", "").split(",") if x.strip().isdigit()])
            cfg.setdefault("webapp_url", os.environ.get("WEBAPP_URL", ""))
            cfg.setdefault("webapp_host", runtime_web_host())
            cfg.setdefault("webapp_port", runtime_web_port())
            cfg.setdefault("webapp_allow_unauth", os.environ.get("WEBAPP_ALLOW_UNAUTH", "false").lower() == "true")
            logger_cfg.info("Config loaded from %s (%d accounts)", CONFIG_FILE, len(cfg.get("accounts", [])))
            return cfg
        except Exception as e:
            logger_cfg.warning("Failed to parse %s: %s, falling back to .env", CONFIG_FILE, e)

    # Fallback: build single-account config from .env vars
    logger_cfg.info("No config.json found, using .env fallback (single account)")
    return {
        "accounts": [{
            "name": "default",
            "api_id": int(os.environ.get("API_ID", os.environ.get("TELEGRAM_API_ID", "0"))),
            "api_hash": os.environ.get("API_HASH", os.environ.get("TELEGRAM_API_HASH", "")),
            "session_name": os.environ.get("SESSION_NAME", "monitor_session"),
            "session_string": os.environ.get("SESSION_STRING", ""),
            "bot_token": os.environ.get("BOT_TOKEN", os.environ.get("TELEGRAM_BOT_TOKEN", "")),
            "phone": os.environ.get("PHONE", ""),
            "target_chat_id": int(os.environ.get("TARGET_CHAT_ID", "0")),
            "target_thread_id": 0,
            "keywords_file": os.environ.get("KEYWORDS_FILE", "keywords.txt"),
            "chats_file": os.environ.get("CHATS_FILE", "chats.txt"),
            "state_file": os.environ.get("STATE_FILE", "state.json"),
        }],
        "registry_file": os.environ.get("REGISTRY_FILE", "lead_registry.json"),
        "outreach_template": os.environ.get(
            "OUTREACH_TEMPLATE",
            "Здравствуйте, {first_name}! Вы писали про «{trigger}» в чате «{chat_title}». "
            "Если вопрос ещё актуален, могу предложить вариант решения. Если сообщения не нужны, напишите «стоп»."
        ),
        "operator_user_ids": [int(x) for x in os.environ.get("OPERATOR_USER_IDS", "").split(",") if x.strip().isdigit()],
        "webapp_url": os.environ.get("WEBAPP_URL", ""),
        "webapp_host": runtime_web_host(),
        "webapp_port": runtime_web_port(),
        "session_string": os.environ.get("SESSION_STRING", ""),
        "webapp_allow_unauth": os.environ.get("WEBAPP_ALLOW_UNAUTH", "false").lower() == "true",
        "routerai_api_key": os.environ.get("ROUTERAI_API_KEY", ""),
        "routerai_model": os.environ.get("ROUTERAI_MODEL", "deepseek/deepseek-v4.1-flash"),
        "dedup_ttl": int(os.environ.get("DEDUP_TTL", "1800")),
        "headless": os.environ.get("HEADLESS", "true").lower() == "true",
    }


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout),
              logging.FileHandler("monitor.log", encoding="utf-8")],
)
logger_cfg = logging.getLogger("lead_monitor.config")
logger_state = logging.getLogger("lead_monitor.state")
logger_patterns = logging.getLogger("lead_monitor.patterns")
logger_web = logging.getLogger("lead_monitor.web")


# ── State Manager ───────────────────────────────────────────────────────────

class State:
    """Persistent JSON state: keywords, target, stats, reactions, dedup."""

    DEDUP_MAX_AGE = 7 * 86400  # 7 days
    DEDUP_MAX_SIZE = 50_000

    def __init__(self, path: str, default_target: int = 0):
        self.path = Path(path)
        self.data = {
            "target_chat_id": default_target,
            "target_thread_id": 0,  # topic/thread ID for forum groups
            "keywords": [],
            "stats": {"total_triggers": 0, "reacted": 0, "contacted": 0, "ai_filtered": 0, "ai_passed": 0},
            "reactions": {},  # alert_msg_id -> {"user": name, "time": ts}
            "sent_hashes": {},  # dedup_hash -> timestamp
            "blacklisted_chats": [],  # chat IDs to ignore
            "leads": {},  # lead_id -> candidate card shown in the inbox
            "outreach_template": "",
            "outreach_mode": "manual",
        }
        self._load()

    def _load(self):
        if self.path.exists():
            try:
                saved = json.loads(self.path.read_text(encoding="utf-8"))
                self.data.update(saved)
                # Ensure sent_hashes exists
                if "sent_hashes" not in self.data:
                    self.data["sent_hashes"] = {}
                if "leads" not in self.data or not isinstance(self.data["leads"], dict):
                    self.data["leads"] = {}
                logger_state.info("State loaded from %s", self.path)
            except Exception:
                logger_state.warning("Corrupt state file, using defaults")

    def save(self):
        """Persist state atomically so a restart cannot read half-written JSON."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(f".{self.path.name}.tmp")
        payload = json.dumps(self.data, ensure_ascii=False, indent=2)
        tmp.write_text(payload, encoding="utf-8")
        tmp.replace(self.path)

    @property
    def target(self) -> int:
        return self.data["target_chat_id"]

    @target.setter
    def target(self, v: int):
        self.data["target_chat_id"] = v
        self.save()

    @property
    def thread_id(self) -> int:
        return self.data.get("target_thread_id", 0)

    @thread_id.setter
    def thread_id(self, v: int):
        self.data["target_thread_id"] = v
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

    # ── Persistent dedup ─────────────────────────────────────────────────────
    def is_sent(self, dedup_hash: str) -> bool:
        """Check if message was already sent (persistent across restarts)."""
        hashes = self.data.get("sent_hashes", {})
        return dedup_hash in hashes

    def mark_sent(self, dedup_hash: str):
        """Mark message as sent with current timestamp."""
        now = int(time.time())
        hashes = self.data.setdefault("sent_hashes", {})
        hashes[dedup_hash] = now
        # Cleanup old entries periodically
        if len(hashes) > self.DEDUP_MAX_SIZE:
            cutoff = now - self.DEDUP_MAX_AGE
            self.data["sent_hashes"] = {k: v for k, v in hashes.items() if v > cutoff}
        self.save()

    def cleanup_old_hashes(self):
        """Remove hashes older than DEDUP_MAX_AGE."""
        now = int(time.time())
        cutoff = now - self.DEDUP_MAX_AGE
        before = len(self.data.get("sent_hashes", {}))
        self.data["sent_hashes"] = {
            k: v for k, v in self.data.get("sent_hashes", {}).items() if v > cutoff
        }
        after = len(self.data["sent_hashes"])
        if before != after:
            logger_state.info("Cleaned up %d old dedup hashes", before - after)
            self.save()

    # ── Blacklist ────────────────────────────────────────────────────────────
    @property
    def blacklisted_chats(self) -> list[int]:
        return self.data.get("blacklisted_chats", [])

    def is_blacklisted(self, chat_id: int) -> bool:
        return chat_id in self.blacklisted_chats

    def blacklist_chat(self, chat_id: int):
        bl = self.data.setdefault("blacklisted_chats", [])
        if chat_id not in bl:
            bl.append(chat_id)
            self.save()

    def unblacklist_chat(self, chat_id: int):
        bl = self.data.get("blacklisted_chats", [])
        if chat_id in bl:
            bl.remove(chat_id)
            self.save()

    # ── AI Stats ─────────────────────────────────────────────────────────────
    def inc_ai_filtered(self):
        self.data["stats"]["ai_filtered"] = self.data["stats"].get("ai_filtered", 0) + 1
        self.save()

    def inc_ai_passed(self):
        self.data["stats"]["ai_passed"] = self.data["stats"].get("ai_passed", 0) + 1
        self.save()

    # ── Lead inbox ───────────────────────────────────────────────────────────
    def add_lead(self, lead_id: str, lead: dict) -> bool:
        """Save a candidate card once. Returns False when it already exists."""
        leads = self.data.setdefault("leads", {})
        if lead_id in leads:
            return False
        leads[lead_id] = lead
        self.save()
        return True

    def get_lead(self, lead_id: str) -> Optional[dict]:
        lead = self.data.get("leads", {}).get(str(lead_id))
        return lead if isinstance(lead, dict) else None

    def update_lead(self, lead_id: str, **changes) -> Optional[dict]:
        lead = self.get_lead(lead_id)
        if not lead:
            return None
        lead.update(changes)
        lead["updated_at"] = int(time.time())
        self.save()
        return lead

    def list_leads(self, status: str = "") -> list[dict]:
        leads = list(self.data.get("leads", {}).values())
        if status:
            leads = [x for x in leads if x.get("status") == status]
        return sorted(leads, key=lambda x: x.get("created_at", 0), reverse=True)

    @property
    def outreach_template(self) -> str:
        return self.data.get("outreach_template", "")

    @outreach_template.setter
    def outreach_template(self, value: str):
        self.data["outreach_template"] = value[:4000]
        self.save()

    @property
    def outreach_mode(self) -> str:
        return self.data.get("outreach_mode", "manual")

    @outreach_mode.setter
    def outreach_mode(self, value: str):
        self.data["outreach_mode"] = value if value in {"manual", "approval"} else "manual"
        self.save()


class ContactRegistry:
    """Global suppression list shared by all monitoring accounts.

    A Telegram username can change, so the numeric user id is the stable key.
    The registry intentionally stores only contact state and timestamps; the
    message excerpt remains in the per-account lead card for operator review.
    """

    def __init__(self, path: str):
        self.path = Path(path)
        self.data = {"users": {}}
        # All accounts in this process share one registry.  Claiming a send is
        # synchronous, so a process-wide lock closes the check-then-send race
        # between callback handlers from different workers.
        self._lock = threading.RLock()
        self._load()

    def _load(self):
        if not self.path.exists():
            return
        try:
            saved = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(saved, dict) and isinstance(saved.get("users"), dict):
                self.data = saved
        except Exception:
            logger_state.warning("Corrupt registry file, using empty registry: %s", self.path)

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.path)

    def get(self, user_id: int) -> Optional[dict]:
        value = self.data.get("users", {}).get(str(user_id))
        return value if isinstance(value, dict) else None

    def register_candidate(self, user_id: int, lead_id: str, account: str) -> bool:
        """Register a user once. False means no new offer should be shown."""
        if not user_id:
            return True
        users = self.data.setdefault("users", {})
        key = str(user_id)
        if key in users:
            return False
        now = int(time.time())
        users[key] = {
            "status": "candidate",
            "lead_id": lead_id,
            "account": account,
            "created_at": now,
            "updated_at": now,
        }
        self.save()
        return True

    def update(self, user_id: int, status: str, **extra):
        if not user_id:
            return
        record = self.data.setdefault("users", {}).setdefault(str(user_id), {})
        record.update(extra)
        record["status"] = status
        record["updated_at"] = int(time.time())
        self.save()

    def claim_send(self, user_id: int, lead_id: str, text_hash: str,
                   account: str = "") -> tuple[bool, str]:
        """Atomically claim a lead before awaiting Telegram's send call.

        ``sending`` is deliberately persistent.  If the process dies or the
        network gives an ambiguous result, a later restart must not blindly
        send the same outreach again.  An operator can clear the record only
        through an explicit administrative action (outside the send path).
        """
        if not user_id:
            return False, "У пользователя нет доступного Telegram ID"
        with self._lock:
            users = self.data.setdefault("users", {})
            key = str(user_id)
            record = users.get(key)
            if record:
                status = str(record.get("status", ""))
                if status in {"sending", "contacted", "blocked", "skipped", "cancelled", "failed"}:
                    return False, "Для этого пользователя отправка уже закрыта"
                old_lead = str(record.get("lead_id") or "")
                if old_lead and old_lead != str(lead_id):
                    return False, "Пользователь уже закреплён за другим лидом"
            now = int(time.time())
            record = dict(record or {})
            record.update({
                "status": "sending",
                "lead_id": str(lead_id),
                "account": account or record.get("account", ""),
                "send_hash": text_hash,
                "send_started_at": now,
                "updated_at": now,
            })
            users[key] = record
            self.save()
            return True, ""

    def is_suppressed(self, user_id: int) -> bool:
        record = self.get(user_id)
        return bool(record and record.get("status") in {
            "candidate", "review", "skipped", "cancelled", "blocked", "contacted", "failed", "sending"
        })


# ── Smart Keyword Engine ─────────────────────────────────────────────────────

def load_lines(fp: str) -> list[str]:
    p = Path(fp)
    if not p.exists():
        return []
    return [l.strip() for l in p.read_text(encoding="utf-8").splitlines()
            if l.strip() and not l.strip().startswith("#")]


# Транслит латиница → кириллица (люди пишут в разной раскладке)
_TRANSLIT_MAP: dict[str, str] = {
    "a": "а", "b": "б", "v": "в", "g": "г", "d": "д", "e": "е",
    "z": "з", "i": "и", "j": "й", "k": "к", "l": "л", "m": "м",
    "n": "н", "o": "о", "p": "п", "r": "р", "s": "с", "t": "т",
    "u": "у", "f": "ф", "h": "х", "c": "к", "y": "у", "x": "кс",
    "w": "в", "q": "к",
    # Частые смешанные замены (латиница выглядит как кириллица)
    "а": "а", "е": "е", "о": "о", "р": "р", "с": "с", "х": "х",
    "у": "у", "к": "к", "м": "м", "т": "т", "в": "в", "н": "н",
}

# Кириллица «a, e, o, p, c, x, y» — омонимы латиницы, уже кирилл, не трогаем.
# Маппинг только для чистой латиницы:
_LAT_TO_CYR = str.maketrans({
    # lowercase latin → cyrillic (explicit ord keys to avoid encoding issues)
    97: "а", 98: "б", 118: "в", 103: "г", 100: "д",   # a b v g d
    122: "з", 105: "и", 106: "й", 107: "к", 108: "л", 109: "м", # z i j k l m
    110: "н", 112: "п", 114: "р", 115: "с", 116: "т", # n p r s t
    117: "у", 102: "ф", 104: "х", 119: "в", 113: "к", # u f h w q
    99: "с", 101: "е", 111: "о", 120: "х", 121: "у",  # c e o x y
    # uppercase latin → cyrillic
    65: "а", 66: "б", 86: "в", 71: "г", 68: "д",      # A B V G D
    90: "з", 73: "и", 74: "й", 75: "к", 76: "л", 77: "м", # Z I J K L M
    78: "н", 80: "п", 82: "р", 83: "с", 84: "т",      # N P R S T
    85: "у", 70: "ф", 72: "х", 87: "в", 81: "к",      # U F H W Q
    67: "с", 69: "е", 79: "о", 88: "х", 89: "у",      # C E O X Y
})

# «Смешанная» латиница-омонимы кириллицы: e→е, o→о, c→с, x→х, y→у
_MIXED_LAT_MAP = str.maketrans({
    "e": "е", "o": "о", "c": "с", "x": "х", "y": "у",
    "E": "е", "O": "о", "C": "с", "X": "х", "Y": "у",
    "a": "а", "A": "а", "p": "р", "P": "р",
})


def normalize(text: str) -> str:
    """
    Нормализация текста:
    1. NFKC + lowercase + ё→е
    2. Убираем zero-width и спец-символы
    3. Конвертируем «смешанные» латиница-омонимы → кириллицу
    4. Слова только из латиницы — транслитерируем в кириллицу
    5. Удаляем не-словарные символы, схлопываем пробелы
    """
    if not text:
        return ""
    t = unicodedata.normalize("NFKC", text).lower()
    t = t.replace("ё", "е").replace("й", "й")
    # Убираем zero-width, мягкий дефис и прочий невидимый мусор
    t = re.sub(r"[\u00ad\u200b-\u200f\u2060\ufeff]", "", t)
    # Обрабатываем по словам
    words = re.split(r"(\s+)", t)
    out = []
    for w in words:
        if re.fullmatch(r"\s+", w):
            out.append(w)
            continue
        # Если слово содержит кириллицу — заменяем только латиница-омонимы
        if re.search(r"[а-яёa-z]", w):
            if re.search(r"[а-яё]", w):
                # смешанное — омонимы латиницы → кирилл
                w = w.translate(_MIXED_LAT_MAP)
            else:
                # чистая латиница — транслитерируем
                w = w.translate(_LAT_TO_CYR)
        out.append(w)
    t = "".join(out)
    # Убираем не-буквы/не-цифры/не-пробелы (пунктуацию, спецсимволы)
    t = re.sub(r"[^\w\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


# ── Паттерны по тематическим кластерам ──────────────────────────────────────
#
# Формат строки keywords.txt:
#   PATTERN:regex — прямой regex-паттерн (применяется к нормализованному тексту)
#   #comment      — комментарий, игнорируется
#   обычная фраза — точное вхождение (как раньше)
#
# Для умных паттернов используем префикс PATTERN:

# Встроенные смарт-кластеры (всегда активны, поверх keywords.txt)
_BUILTIN_SMART_PATTERNS: list[tuple[str, str]] = [
    # ── Лиды / базы ──────────────────────────────────────────────────────────
    ("лиды (общее)",
     r"\bли[дт][ыауе]?\b|\bлидк\w*\b|\blead[sz]?\b|\bлiд\w*\b"),
    ("горячие лиды",
     r"\b(горяч\w*\s+ли[дт]|ли[дт]\w*\s+горяч)\w*\b"),
    ("купить/нужны лиды",
     r"\b(куп\w+|нужн\w+|ищ\w+|продам|продаю|отдам|есть|слив\w+|достать|найти|заказ\w+)\s+\w{0,10}\s*ли[дт]\w*\b"),
    ("база/базы",
     r"\b(баз[аыуе]|базы|базк\w+|bas[eа])\b"),
    ("купить базу",
     r"\b(куп\w+|нужн\w+|ищ\w+|где\s+взять|где\s+брать|продам|продаю|достать|спарсить|скачать)\s+\w{0,15}\s*(баз[аыуе]|базы)\b"),
    ("парсинг/парсер",
     r"\bпарс\w+\b|\bпарсер\b"),
    ("отказники",
     r"\бот(каз|цеп|лип)\w*\b|\bотказник\w*\b"),
    ("контакты/база контактов",
     r"\bконтакт\w*\b|\bбаз[аы]\s+контакт\w*\b|\bищу\s+\w{0,10}\s*контакт\w*\b"),
    ("номера телефонов",
     r"\bномер[аыа]?\s+(телефон\w*|мобильн\w*|сотовых\w*)\b|\bтелефонн\w+\s+баз\w+\b|\bбаз[аы]\s+номер\w*\b|\bномера\s+для\s+обзвон\w*\b"),
    ("слив/утечка данных",
     r"\b(слив|слить|сливаю|утечк\w+|утекл\w+|дамп\w*|dump)\s+\w{0,15}\s*(баз\w*|данн\w*|лид\w*|контакт\w*)\b|\b(баз\w*|данн\w*|лид\w*)\s+\w{0,10}\s*(слив|утечк\w+)\b"),
    ("холодные/тёплые лиды",
     r"\b(холодн\w+|тёпл\w+|тепл\w+|warm|cold)\s+\w{0,10}\s*(ли[дт]\w*|баз\w+)\b"),
    ("валид/проверка базы",
     r"\b(валид\w*|провер\w+|чистк\w+|актуальн\w+|свеж\w+)\s+\w{0,10}\s*(баз\w*|номер\w*|лид\w*)\b"),

    # ── HR / рекрутинг ───────────────────────────────────────────────────────
    ("HR общее",
     r"\bhr\b|\bh[рp]\b|\bэйч\s*ар\b|\bхр\b"),
    ("рекрутинг",
     r"\bрекрут\w+\b"),
    ("подбор персонала",
     r"\bподбор\s+(персонал|сотрудник|кадр|специалист|работник|людей|staff)\w*\b"),
    ("нужен HR/рекрутер",
     r"\b(нужен|ищу|куп\w+|заказ\w+|найти|посовет\w+|кто\s+зан\w+|где\s+найти)\s+\w{0,20}\s*(hr\b|рекрут\w+|подборщ\w+|кадров\w+)\b"),
    ("вакансии/отклики",
     r"\b(отклик|вакансия|вакансии|резюме|кандидат)\w*\b"),
    ("fake/fake hr",
     r"\b(fak[eе]|фейк\w*)\s+\w{0,10}\s*(hr|хр|отклик\w*|резюме|кандидат|собеседован)\b|\b(hr|хр)\s+\w{0,10}\s*(fak[eе]|фейк\w*)\b"),
    ("hh.ru/avito отклики",
     r"\b(hh\.ru|headhunter|авито\s+отклик\w*|avito\s+отклик\w*|отклик\w*\s+\w{0,10}\s*(авито|avito))\b"),
    ("кредитование",
     r"\bкредитовани\w*\b"),
    ("найм сотрудников",
     r"\b(найм|наймем|нанять|трудоустройств)\w*\b"),

    # ── МФО / Кредиты / Финансы ──────────────────────────────────────────────
    ("МФО",
     r"\bмфо\b|\bмикрозайм\w*\b|\bмикрофинанс\w*\b"),
    ("кредитные лиды",
     r"\b(кредит\w*|займ\w*|ссуд\w*)\s+\w{0,10}\s*(ли[дт]\w*|баз\w+|отказ\w+)\b"),
    ("финансовые офферы",
     r"\b(финанс\w+|финк\w+|фино\w+)\s+(оффер|офер|предложени)\w*\b|\bфинансов\w+\s+оффер\w*\b"),
    ("отказники МФО/кредиты",
     r"\bотказник\w*\s+\w{0,10}\s*(мфо|кредит|займ)\w*\b|\b(мфо|кредит|займ)\w*\s+\w{0,10}\s*отказник\w*\b"),
    ("банкротство",
     r"\bбанкротств\w+\b|\bбанкрот\b"),
    ("рко",
     r"\bрко\b|\bрасчетн\w+\s+счет\w*\b|\bоткрыт\w+\s+счет\w*\b"),

    # ── Беттинг / Гемблинг ───────────────────────────────────────────────────
    ("беттинг/ставки",
     r"\b(бетт\w+|ставк\w+|букмек\w+|беттинг\w*|betting|sports?bet|каппер\w*|прогноз\w+\s+на\s+ставк\w*)\b"),
    ("казино/гемблинг",
     r"\b(казино|гемблинг|gambling|слот\w+|онлайн.казино|игров\w+\s+автомат\w*|покер\w*)\b"),
    ("лиды для беттинга",
     r"\bли[дт]\w*\s+\w{0,15}\s*(бетт\w+|ставк\w+|казино|гемблинг)\b|\b(бетт\w+|ставк\w+|казино)\w*\s+\w{0,15}\s*ли[дт]\w*\b"),
    ("депозитчики/игроки",
     r"\b(депозитчик\w*|депозитер\w*|игрок\w*|активн\w+\s+(игрок|клиент|юзер)\w*)\b"),

    # ── Арбитраж трафика ─────────────────────────────────────────────────────
    ("арбитраж",
     r"\bарбитраж\w*\b|\bарб\b"),
    ("трафик",
     r"\b(трафик|траф[fф]?[fф]?|тrafik|trаfik|трофик|трафф?ик?)\w*\b"),
    ("проблемы с трафиком",
     r"\b(проблем\w*|трудност\w*|сложност\w*|вопрос\w*|плох\w*|нет\s+|мало\s+|не\s+хватает|не\s+могу|не\s+нахожу|устал|заебал|пиздец|хуйня|говно|тухлая|мертв\w+)\s+\w{0,15}\s*(трафик|траф[fф]?[fф]?|лид\w*|баз\w*|номер\w*|конверси\w*|обзвон\w*)\w*\b|\b(трафик|траф[fф]?[fф]?|лид\w*|баз\w*)\w*\s+\w{0,15}\s+(проблем\w*|плох\w*|нет\s+|мало\s+|тухлая|мертв\w*|дорог\w*|холодн\w*|выгорел\w*)\b"),
    ("мотивированный трафик",
     r"\b(мотив\w+)\s+\w{0,10}\s*(трафик|траф[fф]?[fф]?)\w*\b|\bмотив\w+\s+(юзер|install|инсталл)\w*\b"),
    ("офферы",
     r"\bоффер\w*\b|\bофер\w+\b"),
    ("партнёрка/CPA",
     r"\b(партнёрка|партнерка|cpa|cpа|аффилиат|affiliate)\w*\b"),
    ("источник/канал трафика",
     r"\b(источник|канал|площадк\w+|сетк\w+|ресурс)\w*\s+\w{0,10}\s*(трафик|траф[fф]?[fф]?)\w*\b|\b(трафик|траф[fф]?[fф]?)\w*\s+\w{0,10}\s*(источник|канал|площадк\w+)\w*\b"),
    ("налить/лить трафик",
     r"\b(налить|лить|лью|льём|налей|залит\w+|слив\w+)\s+\w{0,10}\s*(трафик|траф[fф]?[fф]?)\w*\b"),
    ("покупка/продажа трафика",
     r"\b(куп\w+|продам|продаю|ищ\w+|нужен|нужн\w+|заказ\w+)\s+\w{0,10}\s*(трафик|траф[fф]?[fф]?)\w*\b"),

    # ── ИП / Самозанятые ─────────────────────────────────────────────────────
    ("ИП/самозанятые",
     r"\b(новорег\w*|предрег\w*)\b|\b(новорег\w+|предрег\w+)\s+\w{0,10}\s*ип\b|\bип\w*\s+\w{0,10}\s*(новорег|предрег)\w*\b"),
    ("регистрация ИП",
     r"\b(регистрац\w+|открыт\w+)\s+\w{0,10}\s*ип\b"),

    # ── Курьеры / Персонал ───────────────────────────────────────────────────
    ("курьеры",
     r"\bкурьер\w+\b"),
    ("поиск персонала",
     r"\b(поиск|ищу|нужен|нужны)\s+\w{0,15}\s*(персонал|сотрудник|курьер|работник|специалист)\w*\b"),

    # ── Запрос у автора/поставщика ───────────────────────────────────────────
    ("поставщик баз/лидов",
     r"\bпоставщик\w*\s+\w{0,15}\s*(баз|лид|данных)\w*\b|\b(баз|лид)\w*\s+поставщик\w*\b"),
    ("где купить/найти",
     r"\b(где\s+(купить|взять|брать|найти|достать|заказать)|посовет\w+|кто\s+(продаёт|продает|занимается|может|делает|предлагает))\s+\w{0,25}\s*(баз\w+|лид\w+|hr\b|рекрут\w+|мфо|трафик|персонал)\w*\b"),
    ("продам/слив данных",
     r"\b(продам|продаю|сливаю|слив\b|отдам|есть\s+в\s+наличии|могу\s+предлож\w+)\s+\w{0,15}\s*(баз\w+|лид\w+|данны)\w*\b"),

    # ── Разговорный сленг / неформальные запросы ─────────────────────────────
    ("ищу/нужно (общее)",
     r"\b(ищ\w+|нужн\w+|требу\w+|хочу|надо\s+бы|подскаж\w+|помог\w+\s+найти)\s+\w{0,20}\s*(баз\w+|лид\w+|контакт\w+|номер\w+|клиент\w+|партнёр\w+|оффер\w+)\w*\b"),
    ("есть/знаете ли",
     r"\b(есть\s+(у\s+кого|ли|база|лиды|контакты)|знает\w+\s+(ли\s+кто|где)|кто.нибудь\s+(знает|может|продаёт))\b"),
    ("работа с данными",
     r"\b(работа\w*\s+с\s+\w{0,10}\s*(баз\w+|данн\w+|лид\w+|клиент\w+)|(обработка|обзвон|рассылк\w+)\s+\w{0,10}\s*(баз\w+|номер\w+))\b"),

    # ── ПРОБЛЕМЫ С ТРАФИКОМ / ЛИДАМИ (потенциальные клиенты) ─────────────────
    ("нет лидов / нет конверсий",
     r"\b(нет\s+(лидов?|конверси\w*|результат\w*)|н(у|е)\s+могу\s+найти\s+\w{0,10}\s*(лид\w+|баз\w+|трафик))\b"),
    ("дорогие лиды / трафик",
     r"\b(лид\w*|трафик|траф[fф]?[fф]?)\s+\w{0,10}\s*(дорог\w+|недёшев\w+|космос|заоблачн)\w*\b|\bдорог\w+\s+\w{0,10}\s*(лид\w*|трафик)\b"),
    ("плохая конверсия",
     r"\b(плох\w+|низк\w+|маленьк\w+|слаб\w+|нулев\w+)\s+\w{0,10}\s*(конверси\w*|cr\b|ctr\b|отклик\w*)\b|\bконверси\w*\s+\w{0,10}\s*(плох\w+|низк\w+|упал\w+|просел\w+)\b"),
    ("холодные / тухлые лиды",
     r"\b(холодн\w+|тухл\w+|стар\w+|выгоревш\w+|выгорел\w+|мёртв\w+|мертв\w+)\s+\w{0,10}\s*(лид\w*|баз\w*|номер\w*)\b|\b(лид\w*|баз\w*|номер\w*)\s+\w{0,10}\s*(холодн\w+|тухл\w+|выгорел\w+)\b"),
    ("проблемы с обзвоном",
     r"\b(обзвон\w*\s+\w{0,20}\s*(н(оль|уль)\w*\s+результат|без\s+результат|пуст\w+|никто\s+не\s+(берёт|ответ\w+|отвеча\w+)))\b|\b(никто\s+не\s+(берёт|ответ\w+)\s+\w{0,10}\s*трубк\w*)\b|\b(номер\w*|номера)\s+\w{0,10}\s*(мёртв\w+|не\s+работ\w+|не\s+отвеча\w+)\b|\bобзвон\w*\s+\d+\s+\w{0,10}\s*(номер\w*|лид\w*)\s+\w{0,10}\s*(н(оль|уль)|без|пуст)\w*\b"),
    ("арбитраж не работает",
     r"\b(арбитраж\w*\s+\w{0,10}\s*(не\s+работ\w+|умер\w*|сдох\w*|просел\w*|не\s+вывоз\w*))\b|\b(оффер\w*|офер\w+)\s+\w{0,10}\s*(не\s+конверт\w+|не\s+работ\w+|плох\w+)\b"),
    ("нужен свежий трафик/база",
     r"\b(нужен?\s+\w{0,10}\s*(свеж\w+|жив\w+|рабоч\w+|нормальн\w+|качественн\w+)\s+\w{0,10}\s*(трафик|лид\w*|баз\w*|номер\w*))\b|\b(где\s+\w{0,10}\s*(взять|брать|достать|найти)\s+\w{0,10}\s*(свеж\w+|нормальн\w+)\s+\w{0,10}\s*(баз\w*|лид\w*))\b"),
    ("конверт/cr просел",
     r"\b(конверт\w*|ср|cr)\s+\w{0,15}\s*(упал\w*|просел\w*|низк\w*|плох\w*|нул\w*|минимальн\w*)\b|\b(упал\w*|просел\w*)\s+\w{0,10}\s*(конверт\w*|ср|cr)\b|\b(ср|cr)\s+\d+[\.,]?\d*\s*%?\b"),
    ("дозвон проблемы",
     r"\b(дозвон\w*|до\s*звон\w*)\s+\w{0,15}\s*(\d+\s*%?|н(оль|уль)\w*|пуст\w*|без\s+результат|плох\w*|низк\w*|пипец|ужас\w*)\b|\b(номера?|номер\w*)\s+\w{0,10}\s*(биты\w+|убиты\w+|мёртв\w+|мертв\w+|не\s+рабоч\w+)\b"),
]

# Компилируем встроенные паттерны один раз
_COMPILED_BUILTIN: list[tuple[str, re.Pattern]] = [
    (name, re.compile(pat, re.IGNORECASE | re.UNICODE))
    for name, pat in _BUILTIN_SMART_PATTERNS
]

# ── Негативный pre-filter: отсекает очевидные бытовые FP без AI-запроса ──────
# ВАЖНО: паттерны применяются к НОРМАЛИЗОВАННОМУ тексту (латиница→кириллица).
# "mysql"→"мускл", "json"→"жсон", "ozon"→"озон", "hr"→"хр", "job"→"джоб"
_FALSE_POSITIVE_PATTERNS: list[re.Pattern] = [
    # база в бытовом контексте
    re.compile(r"\b(баз[аыуе]|базы)\s+\w{0,10}\s*(отдых|знани|данных\s+(мускл|постгрес|скл|sql)|военн|продуктов|склад|логистик|грузов|товарн|цен|тариф|комплектаци|математик|физик|химии|английск|программирован)", re.I),
    re.compile(r"\b(налогов|военн|продуктов|товарн|грузов|логистик|складск)\w*\s+\w{0,5}\s*(баз[аыуе]|базы)\b", re.I),
    re.compile(r"\bбазов\w+\s+(набор|тариф|комплект|уровень|ставк|план|пакет|функционал|верси)", re.I),
    # трафик в бытовом контексте
    re.compile(r"\b(трафик|траф[fф]?[fф]?)\s+\w{0,10}\s*(на\s+(мкад|дорог|трасс|шоссе|улиц)|интернет|мобильн|дорожн|сайт\w*\s+(вырос|упал|растёт)|вайфай|сетев|роутер)", re.I),
    re.compile(r"\b(дорожн|интернет|мобильн|сетев|вайфай)\w*\s+\w{0,5}\s*(трафик|траф[fф]?[fф]?)\b", re.I),
    # контакт в бытовом контексте
    re.compile(r"\bконтакт\w*\s+\w{0,10}\s*(линз|сварк|площадк|детал|провод|пластин|ребёнк|детьми|животн)", re.I),
    re.compile(r"\b(линз|сварк|площадк)\w*\s+\w{0,5}\s*контакт\w*", re.I),
    re.compile(r"\bконтакт\s+с\s+(ребенк|ребёнк|детьми|человек|животн|природ)", re.I),
    re.compile(r"\bконтакт\w*\s+в\s+(телефон|записн|блокнот|адресн)", re.I),
    re.compile(r"\b(сохран|запиш|добав)\w*\s+\w{0,10}\s*контакт", re.I),
    # арбитраж юридический
    re.compile(r"\b(арбитражн\w+|третейск\w+)\s+\w{0,10}\s*(суд|разбирательств|процесс|решени|заседани)", re.I),
    re.compile(r"\b(суд|разбирательств)\w*\s+\w{0,5}\s*арбитраж", re.I),
    # парсинг программирование (нормализованные варианты)
    re.compile(r"\bпарс\w+\s+\w{0,10}\s*(жсон|xml|html|csv|excel|pdf|апи|бьютифулсоуп|беаутифулсоуп|селениум|скрапи|lxml|xpath|css|сайта?\s+для\s+(учёб|курс|практик))", re.I),
    re.compile(r"\b(жсон|xml|html|csv|хтмл)\s+парс", re.I),
    # курьер доставка (ozon→озон, wildberries→вилдберрис)
    re.compile(r"\bкурьер\w*\s+\w{0,15}\s*(доставк|озон|вилдберрис|яндекс\s+еда|яндекс\s+лавка|самокат|сдек|почт|dpd|боксберри)", re.I),
    re.compile(r"\b(озон|вилдберрис|яндекс\s+еда|сдек|почт\w*)\w*\s+\w{0,10}\s*курьер", re.I),
    re.compile(r"\bкурьерск\w+\s+доставк", re.I),
    # ставки финансовые
    re.compile(r"\bставк\w*\s+\w{0,10}\s*(ипотек|кредитн|процентн|цб|центрбанк|ключев|рефинансирован|налог\w*|ндс|депозитн)", re.I),
    re.compile(r"\b(ипотечн|кредитн|процентн|ключев)\w*\s+\w{0,5}\s*ставк", re.I),
    # оффер работа (job→джоб, offer→оффер)
    re.compile(r"\b(оффер)\w*\s+\w{0,10}\s*(от\s+(яндекс|гугл|сбер|тинькофф|озон|вк|мейл|мегафон|билайн|мтс)|работ|зарплат|собеседован|хр\s+прислал|рекрутер)", re.I),
    re.compile(r"\b(джоб|ворк)\s+оффер", re.I),
    # банкротство процедура
    re.compile(r"\bбанкротств\w*\s+\w{0,10}\s*(компани|процедур|закон|суд|дело|гражданск|физическ|юридическ|этап|стади)", re.I),
    # мфо личный займ
    re.compile(r"\bмфо\w*\s+\w{0,10}\s*(взял|взяла|оформил|получил|погасил|закрыл|платеж|платёж|долг|просрочк|штраф|пеню)", re.I),
    # номер бытовой
    re.compile(r"\bномер\w*\s+\w{0,10}\s*(заказ|квартир|дом|комнат|этаж|мест|билет|паспорт|полис|счёт|счет|авто|машины)", re.I),
    # отказник юридический
    re.compile(r"\bотказник\w*\s+\w{0,10}\s*(от\s+(наследств|имуществ|доли|прав|алимент|гражданств)|беженец|переселенец)", re.I),
    # hr отдел/бренд (hr→хр)
    re.compile(r"\bхр\s+\w{0,10}\s*(отдел|департамент|бренд|специалист\s+нашей|менеджер\s+нашей|директор|служба|команд)", re.I),
]


def build_pattern(kws: list[str]) -> re.Pattern:
    """Строит паттерн из обычных ключевых фраз (keywords.txt без PATTERN:)."""
    plain = []
    for k in kws:
        if k.startswith("PATTERN:"):
            continue  # обрабатываются отдельно в find_trigger
        plain.append(k)
    if not plain:
        # Фиктивный паттерн, который ничего не найдёт (но не падает)
        return re.compile(r"(?!)", re.IGNORECASE)
    plain = sorted(plain, key=len, reverse=True)
    return re.compile("|".join(re.escape(normalize(k)) for k in plain), re.IGNORECASE)


def build_custom_patterns(kws: list[str]) -> list[tuple[str, re.Pattern]]:
    """Строит список именованных паттернов из строк PATTERN:name=regex."""
    result = []
    for k in kws:
        if not k.startswith("PATTERN:"):
            continue
        rest = k[len("PATTERN:"):]
        if "=" in rest:
            name, _, pat = rest.partition("=")
            try:
                result.append((name.strip(), re.compile(pat.strip(), re.IGNORECASE | re.UNICODE)))
            except re.error as ex:
                logger_patterns.warning("Bad PATTERN '%s': %s", name, ex)
    return result


def find_trigger(normalized_text: str, plain_pat: re.Pattern,
                 custom_pats: list[tuple[str, re.Pattern]]) -> Optional[str]:
    """
    Ищет первый сработавший триггер в нормализованном тексте.
    Возвращает человекочитаемое название триггера или None.

    Порядок проверки:
      0. Негативный pre-filter (бытовые FP) → None
      1. Встроенные смарт-кластеры (_COMPILED_BUILTIN)
      2. Пользовательские PATTERN: строки из keywords.txt
      3. Точные ключевые фразы из keywords.txt
    """
    # Pre-filter: отсекаем очевидные бытовые FP без AI-запроса
    for fp_pat in _FALSE_POSITIVE_PATTERNS:
        if fp_pat.search(normalized_text):
            return None

    for name, pat in _COMPILED_BUILTIN:
        m = pat.search(normalized_text)
        if m:
            return f"{name} [{m.group(0)}]"

    for name, pat in custom_pats:
        m = pat.search(normalized_text)
        if m:
            return f"{name} [{m.group(0)}]"

    m = plain_pat.search(normalized_text)
    if m:
        return m.group(0)

    return None


def dedup_key(cid: int, text: str) -> str:
    """Legacy text-based dedup key (kept for compatibility)."""
    return hashlib.sha256(f"{cid}:{normalize(text)}".encode()).hexdigest()[:16]


def msg_dedup_key(chat_id: int, message_id: int) -> str:
    """Unique key per message — never collides for different messages."""
    return f"{chat_id}:{message_id}"


def msg_link(chat, mid: int) -> str:
    if isinstance(chat, Channel) and chat.username:
        return f"https://t.me/{chat.username}/{mid}"
    return f"https://t.me/c/{chat.id}/{mid}"


def render_outreach_template(template: str, lead: dict) -> str:
    """Render only the documented, harmless placeholders from a lead card."""
    sender = lead.get("sender", {})
    values = {
        "first_name": sender.get("first_name") or "коллега",
        "last_name": sender.get("last_name") or "",
        "username": sender.get("username") or "",
        "user_id": str(lead.get("user_id") or ""),
        "trigger": lead.get("trigger") or "",
        "topic": lead.get("trigger") or "",
        "chat_title": lead.get("chat_title") or "чат",
        "message": lead.get("message") or "",
    }
    class SafeDict(dict):
        def __missing__(self, key):
            return ""
    try:
        normalized_template = template.replace("{{", "{").replace("}}", "}")
        text = normalized_template.format_map(SafeDict(values))
    except (ValueError, KeyError):
        text = template
    return text.strip()[:4000]


def validate_webapp_init_data(init_data: str, bot_token: str, max_age: int = 86400) -> Optional[dict]:
    """Validate Telegram Mini App initData and return the embedded user object."""
    if not init_data or not bot_token:
        return None
    try:
        from urllib.parse import parse_qsl
        raw_pairs = parse_qsl(init_data, keep_blank_values=True, strict_parsing=True)
        # A dict() conversion silently accepts duplicate keys.  Reject them:
        # otherwise an attacker can authenticate one value while the server
        # verifies another representation of the same initData string.
        if not raw_pairs or len({key for key, _ in raw_pairs}) != len(raw_pairs):
            return None
        pairs = dict(raw_pairs)
        received_hash = pairs.pop("hash", "")
        auth_date = int(pairs.get("auth_date", "0"))
        now = time.time()
        # Telegram timestamps are short-lived.  Reject future timestamps as
        # well as stale ones; a small clock-skew allowance avoids false rejects
        # on VPS hosts whose clock is a few minutes ahead.
        if (not received_hash or not auth_date or
                now - auth_date > max_age or auth_date > now + 300):
            return None
        check_string = "\n".join(f"{k}={pairs[k]}" for k in sorted(pairs))
        secret = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
        expected = hmac.new(secret, check_string.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, received_hash):
            return None
        user = json.loads(pairs.get("user", "{}"))
        return user if isinstance(user, dict) else None
    except (ValueError, TypeError, json.JSONDecodeError):
        return None


# ── Bot API Client ──────────────────────────────────────────────────────────

class BotAPI:
    BASE = "https://api.telegram.org/bot{token}/{method}"

    def __init__(self, token: str, state: State, account_name: str = "default",
                 registry: Optional[ContactRegistry] = None,
                 operator_user_ids: Optional[list[int]] = None):
        self.token = token
        self.state = state
        self.account_name = account_name
        self.registry = registry
        self.operator_user_ids = {int(x) for x in (operator_user_ids or []) if str(x).isdigit()}
        self.logger = logging.getLogger(f"lead_monitor.bot.{account_name}")
        self._session: Optional[aiohttp.ClientSession] = None
        self._offset = 0
        self._running = False
        self._chats_cache: list[tuple[str, int]] = []  # [(name, chat_id), ...]
        self._auth_callback = None  # async callable(code_str) for auth flow
        self._auth_trigger = None   # async callable(chat_id) to start auth
        self._status_callback = None  # async callable() -> str
        self._send_callback = None   # async callable(lead_id) -> (ok, message)
        self._patterns_refresh_callback = None

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
                    # Handle supergroup migration
                    params = data.get("parameters", {})
                    new_cid = params.get("migrate_to_chat_id")
                    if new_cid:
                        self.logger.info("Chat migrated to %s, updating target", new_cid)
                        self.state.target = new_cid
                        # Retry with new chat_id
                        if "chat_id" in kw:
                            kw["chat_id"] = new_cid
                        async with s.post(url, json=kw, timeout=aiohttp.ClientTimeout(total=30)) as r2:
                            data = await r2.json()
                        return data
                    self.logger.error("Bot API %s: %s", method, data)
                return data
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger.exception("Bot API %s failed", method)
            return {}

    async def send_alert(self, chat_id: int, body: str, thread_id: int = 0,
                         lead_id: str = "") -> Optional[int]:
        if lead_id:
            kb = {"inline_keyboard": [
                [{"text": "✉️ Предложить отправку", "callback_data": f"offer:{lead_id}"}],
                [
                    {"text": "⏭ Пропустить", "callback_data": f"skip:{lead_id}"},
                    {"text": "🚫 Не связываться", "callback_data": f"block:{lead_id}"},
                ],
            ]}
            # The Mini App button is optional: Telegram requires a public HTTPS URL.
            mini_url = getattr(self, "mini_app_url", "")
            if mini_url and chat_id > 0:
                sep = "&" if "?" in mini_url else "?"
                kb["inline_keyboard"].append([{
                    "text": "📱 Открыть Inbox",
                    "web_app": {"url": f"{mini_url}{sep}lead={lead_id}"},
                }])
        else:
            kb = {"inline_keyboard": [[
                {"text": "✅ Отреагировать", "callback_data": "react"}
            ]]}
        kw = {"chat_id": chat_id, "text": body,
              "parse_mode": "HTML", "disable_web_page_preview": True,
              "reply_markup": kb}
        if thread_id:
            kw["message_thread_id"] = thread_id
        resp = await self.api("sendMessage", **kw)
        if resp.get("ok"):
            return resp["result"]["message_id"]
        else:
            self.logger.error("send_alert FAILED to %s (thread=%s): %s", chat_id, thread_id, resp)
            return None

    async def edit_msg(self, chat_id: int, msg_id: int, text: str, kb=None):
        kw = {"chat_id": chat_id, "message_id": msg_id, "text": text,
              "parse_mode": "HTML", "disable_web_page_preview": True}
        if kb:
            kw["reply_markup"] = kb
        await self.api("editMessageText", **kw)

    async def answer_cb(self, cb_id: str, text: str = ""):
        await self.api("answerCallbackQuery", callback_query_id=cb_id, text=text)

    async def send(self, chat_id: int, text: str, reply_markup=None):
        kw = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
        if reply_markup:
            kw["reply_markup"] = reply_markup
        await self.api("sendMessage", **kw)

    def _is_operator(self, user_id: int) -> bool:
        # An empty allow-list is a safe misconfiguration: it denies operator
        # actions instead of turning the bot into a public control panel.
        return bool(self.operator_user_ids) and int(user_id or 0) in self.operator_user_ids

    async def _handle_cmd(self, msg: dict):
        text = msg.get("text", "").strip()
        sender_id = int(msg.get("from", {}).get("id", 0) or 0)
        if text.lower() in {"/stop", "/unsubscribe", "стоп", "отписка"}:
            if self.registry:
                self.registry.update(sender_id, "blocked", reason="user_opt_out")
            await self.send(msg["chat"]["id"], "✅ Больше не буду предлагать вам сообщения.")
            return
        if not self._is_operator(sender_id):
            return
        # Strip @BotName suffix from commands
        if "@" in text.split()[0]:
            text = text.split("@")[0] + (" " + " ".join(text.split()[1:]) if len(text.split()) > 1 else "")
        text = text.strip()
        cid = msg["chat"]["id"]
        if text in ("/start", "/help"):
            await self.send(cid,
                "\U0001f916 <b>Lead Monitor — Lead Inbox</b>\n\n"
                "/auth — авторизация Telethon\n"
                "/target <code>ID</code> — чат для алертов\n"
                "/keywords — триггеры\n"
                "/addkeyword <code>фраза</code>\n"
                "/delkeyword <code>фраза</code>\n"
                "/template — текущий шаблон\n"
                "/settemplate <code>текст</code> — изменить шаблон\n"
                "/miniapp — ссылка на Lead Inbox\n"
                "/chats — мониторимые чаты\n"
                "/stats — статистика + AI\n"
                "/status — статус подключения\n"
                "/blacklist <code>ID</code> — исключить чат\n"
                "/unblacklist <code>ID</code> — вернуть чат")
        elif text == "/auth":
            # Delegate to LeadMonitor via callback (run as separate task!)
            if self._auth_trigger:
                asyncio.create_task(self._auth_trigger(cid))
            else:
                await self.send(cid, "\u274c Auth not available")
        elif text.startswith("/target"):
            parts = text.split(maxsplit=1)
            if len(parts) < 2:
                await self.send(cid, f"Target: <code>{self.state.target}</code>\n\nUse /target <code>ID</code> or /settarget")
            else:
                try:
                    self.state.target = int(parts[1]); await self.send(cid, f"\u2705 Target set to <code>{parts[1]}</code>")
                except ValueError:
                    await self.send(cid, "\u274c Bad ID")
        elif text == "/testalert":
            t = self.state.target
            if not t:
                await self.send(cid, "\u274c No target set. Use /target first")
            else:
                tid = self.state.thread_id
                await self.send(cid, f"\U0001f680 Sending test alert to <code>{t}</code> (thread={tid})...")
                mid = await self.send_alert(t, "\U0001f9ea <b>Test Alert</b>\n\nIf you see this, target is working!", tid)
                if mid:
                    await self.send(cid, f"\u2705 Sent! Message ID: {mid}")
                else:
                    await self.send(cid, "\u274c Failed to send. Check logs for details.")
        elif text == "/chatid":
            chat = msg.get("chat", {})
            title = chat.get("title") or chat.get("first_name") or "?"
            thread = msg.get("message_thread_id", 0)
            txt = f"\U0001f4cb <b>{html.escape(title)}</b>\nChat ID: <code>{cid}</code>"
            if thread:
                txt += f"\nThread/Topic ID: <code>{thread}</code>"
            await self.send(cid, txt)
        elif text.startswith("/thread"):
            parts = text.split(maxsplit=1)
            if len(parts) < 2:
                await self.send(cid, f"Current thread_id: <code>{self.state.thread_id}</code>\n\nUse /thread <code>ID</code> (get it via /chatid in the topic)")
            else:
                try:
                    self.state.thread_id = int(parts[1])
                    await self.send(cid, f"\u2705 Thread ID set to <code>{parts[1]}</code>")
                except ValueError:
                    await self.send(cid, "\u274c Bad ID")
        elif text == "/settarget":
            # Show recent chats with inline buttons
            if not self._chats_cache:
                await self.send(cid, "No chats discovered yet. Wait for monitor to start.")
            else:
                kb_rows = []
                for i, (chat_name, chat_id) in enumerate(self._chats_cache[:20]):
                    kb_rows.append([{"text": chat_name[:60], "callback_data": f"settarget:{chat_id}"}])
                kb = {"inline_keyboard": kb_rows}
                await self.send(cid, "\U0001f4cb Select target chat:", reply_markup=kb)
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
                    if self._patterns_refresh_callback:
                        self._patterns_refresh_callback()
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
                    self.state.keywords = nk
                    if self._patterns_refresh_callback:
                        self._patterns_refresh_callback()
                    await self.send(cid, f"\U0001f5d1 <code>{html.escape(kw)}</code>")
                else:
                    await self.send(cid, "\u274c Not found")
            else:
                await self.send(cid, "/delkeyword phrase")
        elif text == "/template":
            template = self.state.outreach_template or "(не задан)"
            await self.send(cid, f"✉️ <b>Шаблон</b>\n\n<blockquote>{html.escape(template)}</blockquote>\n\nПеременные: {{first_name}}, {{username}}, {{trigger}}, {{chat_title}}, {{message}}")
        elif text.startswith("/settemplate"):
            template = text.split(maxsplit=1)[1].strip() if len(text.split(maxsplit=1)) > 1 else ""
            if template:
                self.state.outreach_template = template
                await self.send(cid, "✅ Шаблон сохранён")
            else:
                await self.send(cid, "/settemplate текст сообщения")
        elif text == "/miniapp":
            url = getattr(self, "mini_app_url", "")
            await self.send(cid, f"📱 <a href=\"{html.escape(url, quote=True)}\">Открыть Lead Inbox</a>" if url else "Mini App URL пока не задан. Укажите WEBAPP_URL.")
        elif text == "/chats":
            if self._chats_cache:
                lines = [f"\u2022 {html.escape(n)} (<code>{cid}</code>)" for n, cid in self._chats_cache[:50]]
                await self.send(cid, "\n".join(lines))
            else:
                await self.send(cid, "Not loaded yet")
        elif text == "/stats":
            s = self.state.stats
            t = s.get("total_triggers", 0)
            r = s.get("reacted", 0)
            ai_f = s.get("ai_filtered", 0)
            ai_p = s.get("ai_passed", 0)
            contacted = s.get("contacted", 0)
            p = round(r/t*100, 1) if t else 0
            bl = len(self.state.blacklisted_chats)
            pending = len(self.state.list_leads("new")) + len(self.state.list_leads("review"))
            await self.send(cid,
                f"📊 <b>Статистика</b>\n\n"
                f"🔔 Триггеры: {t}\n"
                f"✅ Реагировали: {r} ({p}%)\n"
                f"✉️ Отправлено вручную: {contacted}\n"
                f"🟡 Ждут решения: {pending}\n"
                f"🤖 AI отфильтровано: {ai_f}\n"
                f"🤖 AI пропущено: {ai_p}\n"
                f"🚫 Чатов в блэклисте: {bl}")
        elif text.startswith("/blacklist"):
            parts = text.split(maxsplit=1)
            if len(parts) < 2:
                bl = self.state.blacklisted_chats
                if bl:
                    lines = [f"• <code>{c}</code>" for c in bl]
                    await self.send(cid, "🚫 <b>Blacklisted chats:</b>\n" + "\n".join(lines))
                else:
                    await self.send(cid, "Blacklist is empty. Use /blacklist <code>chat_id</code>")
            else:
                try:
                    bl_id = int(parts[1])
                    self.state.blacklist_chat(bl_id)
                    await self.send(cid, f"✅ Chat <code>{bl_id}</code> blacklisted")
                except ValueError:
                    await self.send(cid, "❌ Bad ID")
        elif text.startswith("/unblacklist"):
            parts = text.split(maxsplit=1)
            if len(parts) < 2:
                await self.send(cid, "/unblacklist <code>chat_id</code>")
            else:
                try:
                    bl_id = int(parts[1])
                    self.state.unblacklist_chat(bl_id)
                    await self.send(cid, f"✅ Chat <code>{bl_id}</code> removed from blacklist")
                except ValueError:
                    await self.send(cid, "❌ Bad ID")
        elif text == "/status":
            if self._status_callback:
                status = await self._status_callback()
                await self.send(cid, status)
            else:
                await self.send(cid, "\u274c Status not available")
        else:
            return
        # If we got here without sending anything for a known command, log it
    async def _handle_cmd_safe(self, msg: dict):
        try:
            await self._handle_cmd(msg)
        except Exception as e:
            self.logger.exception("Command handler error")
            cid = msg.get("chat", {}).get("id")
            if cid:
                await self.send(cid, f"\u274c Error: {e}")

    async def _handle_cb(self, cb: dict):
        if not self._is_operator(cb.get("from", {}).get("id", 0)):
            await self.answer_cb(cb.get("id", ""), "Нет доступа")
            return
        cid = cb["message"]["chat"]["id"]
        mid = cb["message"]["message_id"]
        un = cb.get("from", {}).get("username") or cb.get("from", {}).get("first_name", "?")
        data = cb.get("data", "")
        if data.startswith("offer:"):
            lead_id = data.split(":", 1)[1]
            lead = self.state.get_lead(lead_id)
            if not lead:
                await self.answer_cb(cb["id"], "Лид уже недоступен")
                return
            if lead.get("status") in {"blocked", "contacted", "skipped", "cancelled", "sending", "failed"}:
                await self.answer_cb(cb["id"], "Для этого пользователя отправка закрыта")
                return
            self.state.update_lead(lead_id, status="review", offered_at=int(time.time()))
            if self.registry:
                self.registry.update(int(lead.get("user_id") or 0), "review", lead_id=lead_id)
            template = self.state.outreach_template or "Здравствуйте, {first_name}! Вы писали про «{trigger}» в чате «{chat_title}». Если актуально, могу помочь. Если сообщения не нужны, напишите «стоп»."
            preview = render_outreach_template(template, lead)
            # Keep the exact text shown to the operator.  Confirmation later
            # must not re-render against a template changed in the meantime.
            self.state.update_lead(lead_id, preview_text=preview,
                                   preview_hash=hashlib.sha256(preview.encode()).hexdigest())
            kb = {"inline_keyboard": [[
                {"text": "✅ Подтвердить отправку", "callback_data": f"send:{lead_id}"},
                {"text": "✏️ Изменить шаблон", "callback_data": "template"},
            ], [{"text": "❌ Отмена", "callback_data": f"cancel:{lead_id}"}]]}
            await self.answer_cb(cb["id"], "Подготовил сообщение")
            await self.send(cid, f"✉️ <b>Предпросмотр сообщения</b>\n\n<blockquote>{html.escape(preview)}</blockquote>", kb)
        elif data.startswith("send:"):
            lead_id = data.split(":", 1)[1]
            if not self._send_callback:
                await self.answer_cb(cb["id"], "Отправка недоступна")
                return
            await self.answer_cb(cb["id"], "Отправляю…")
            ok, result = await self._send_callback(lead_id)
            await self.send(cid, (f"✅ {html.escape(result)}" if ok else f"❌ {html.escape(result)}"))
        elif data.startswith("cancel:"):
            lead_id = data.split(":", 1)[1]
            lead = self.state.get_lead(lead_id) or {}
            self.state.update_lead(lead_id, status="cancelled", cancelled_at=int(time.time()))
            if self.registry:
                self.registry.update(int(lead.get("user_id") or 0), "cancelled", lead_id=lead_id)
            await self.answer_cb(cb["id"], "Отменено")
        elif data.startswith("skip:"):
            lead_id = data.split(":", 1)[1]
            if self.state.update_lead(lead_id, status="skipped", skipped_at=int(time.time())):
                lead = self.state.get_lead(lead_id) or {}
                if self.registry:
                    self.registry.update(int(lead.get("user_id") or 0), "skipped", lead_id=lead_id)
            await self.answer_cb(cb["id"], "Пропущено")
        elif data.startswith("block:"):
            lead_id = data.split(":", 1)[1]
            lead = self.state.get_lead(lead_id) or {}
            self.state.update_lead(lead_id, status="blocked", blocked_at=int(time.time()))
            if self.registry:
                self.registry.update(int(lead.get("user_id") or 0), "blocked", lead_id=lead_id)
            await self.answer_cb(cb["id"], "Добавлено в список не связываться")
        elif data == "template":
            await self.answer_cb(cb["id"], "Измените OUTREACH_TEMPLATE в настройках")
        elif data == "react":
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
        elif data.startswith("settarget:"):
            try:
                target_id = int(data.split(":")[1])
                self.state.target = target_id
                await self.answer_cb(cb["id"], f"\u2705 Target set!")
                await self.send(cid, f"\u2705 Target chat set to <code>{target_id}</code>")
            except (ValueError, IndexError):
                await self.answer_cb(cb["id"], "\u274c Error")

    async def poll(self):
        self._running = True
        self.logger.info("Bot polling started")
        while self._running:
            try:
                resp = await self.api("getUpdates", offset=self._offset, timeout=30)
                for upd in resp.get("result", []):
                    self._offset = upd["update_id"] + 1
                    if "message" in upd:
                        msg = upd["message"]
                        txt = msg.get("text", "").strip()
                        if txt.startswith("/"):
                            await self._handle_cmd_safe(msg)
                        elif txt.lower() in {"стоп", "stop", "отписка", "unsubscribe"}:
                            if self.registry:
                                sender_id = int(msg.get("from", {}).get("id", 0) or 0)
                                self.registry.update(sender_id, "blocked", reason="user_opt_out")
                            await self.send(msg["chat"]["id"], "✅ Больше не буду предлагать вам сообщения.")
                        elif self._auth_callback and txt:
                            # During auth flow, any non-command text = auth code
                            await self._auth_callback(txt)
                    elif "callback_query" in upd:
                        await self._handle_cb(upd["callback_query"])
            except asyncio.CancelledError:
                break
            except Exception:
                self.logger.exception("Poll error")
                await asyncio.sleep(5)

    def stop(self):
        self._running = False

    async def close(self):
        self.stop()
        if self._session and not self._session.closed:
            await self._session.close()


# ── RouterAI Lead Classifier ────────────────────────────────────────────────

logger_ai = logging.getLogger("lead_monitor.ai")

_CLASSIFY_SYSTEM = """Ты — эксперт-классификатор для Telegram-чатов. Мы продаём КОНКРЕТНЫЕ базы данных и лиды:

📦 НАШИ ПРОДУКТЫ (только это интересно):
1. Отклики с вакансий (база соискателей, резюме)
2. Отказники МФО (люди которым отказали в микрозайме)
3. Отказники по кредитам (люди которым отказал банк)
4. Казино (лиды игроков, депозитчики, база казино)
5. Букмекерские конторы (лиды бетторов, игроки БК)
6. Новореги ИП (только что зарегистрированные ИП)
7. Предреги ИП (предварительная регистрация ИП)
8. Лиды под банкротство (должники, физлица на банкротство)
9. Курьеры (база курьеров для доставки)
10. Банковские лиды (РКО, открытие счетов для бизнеса)

🎯 ТИП 1 — ПОКУПАТЕЛЬ (хочет купить у нас):
Человек ИЩЕТ или СПРАШИВАЕТ про наши продукты.
Примеры: "где взять базу отказников мфо", "нужны новореги ип", "ищу лиды под банкротство",
"куплю базу курьеров", "кто продаёт отклики с вакансий", "нужна база казино".

🎯 ТИП 2 — ПРОДАВЕЦ (конкурент/партнёр):
Человек ПРОДАЁТ именно наши продукты (базы/лиды из списка выше).
Примеры: "продам базу отказников мфо", "есть свежие новореги ип", "слив базы банкротов".

🎯 ТИП 3 — ЧЕЛОВЕК С ПРОБЛЕМОЙ (потенциальный клиент):
У человека ПРОБЛЕМЫ с трафиком, лидами, базами, обзвоном, конверсией. Ему можно предложить наши базы.
Примеры: "проблемы с трафиком", "не могу найти нормальные лиды", "база тухлая",
"трафик дорогой а конверсий нет", "лиды холодные", "обзвонил 500 номеров ноль результата",
"нет лидов", "лиды дорогие", "плохая конверсия", "база выгорела".
ВАЖНО: даже если просто жалуется на проблему без конкретного вопроса — это ЛИД, ему можно помочь.

❌ НЕ ИНТЕРЕСНО (false positive):
- USDT/крипта обмен, P2P, OTC, flash tokens, fake crypto
- SMM, рассылки, парсинг чатов, продвижение каналов, инвайтинг
- Вакансии риэлтора, ассистента, удалённая работа за 3000₽
- Продажа подписок на ChatGPT/Claude/Gemini/Cursor
- Рекламный спам услуг (дизайн, сайты, боты, скрипты)
- "База отдыха", "базовый тариф", "база знаний", "база по математике"
- Просто упоминание слова "трафик", "база", "контакт" без контекста покупки/продажи наших продуктов
- Набор в команду траферов/воркеров (это не покупка базы)
- Предложения раскрутки, рекламы, пиара
- Карты, дропы, верификация аккаунтов, аренда симок
- Любые офферы не связанные с нашими 10 продуктами

Отвечай СТРОГО в JSON (без markdown, без пояснений):
{"is_lead": true/false, "confidence": 0.0-1.0, "reason": "коротко почему"}"""

_CLASSIFY_EXAMPLES = [
    {"role": "user", "content": "[Триггер: 'отказники мфо']\nпродам базу отказников мфо свежие 50к номеров"},
    {"role": "assistant", "content": '{"is_lead": true, "confidence": 0.98, "reason": "продавец базы отказников МФО — наш продукт"}'},
    {"role": "user", "content": "[Триггер: 'новореги ип']\nгде взять свежую базу новорегов ип?"},
    {"role": "assistant", "content": '{"is_lead": true, "confidence": 0.97, "reason": "покупатель ищет базу новорегов ИП — наш продукт"}'},
    {"role": "user", "content": "[Триггер: 'база/базы [базы]']\n🦬БАЗЫ ФИЗЛИЦ 🐗 Гео:РФ, ЕУ Банки Интернет магазины Ювелирка"},
    {"role": "assistant", "content": '{"is_lead": false, "confidence": 0.95, "reason": "продажа баз физлиц для рассылок/спама, не наши продукты"}'},
    {"role": "user", "content": "[Триггер: 'трафик [траферы]']\nОБЪЯВЛЯЮ НАБОР В ТИМУ ТРЕБУЮТСЯ ТРАФЕРЫ И ВОРКЕРЫ оплата от 40$/человек"},
    {"role": "assistant", "content": '{"is_lead": false, "confidence": 0.96, "reason": "набор в команду траферов, не покупка базы"}'},
    {"role": "user", "content": "[Триггер: 'вакансии/отклики [откликов]']\nУДАЛЁННАЯ РАБОТА 3000₽ ЗА СМЕНУ Ищем ассистентов риэлтора размещение объявлений на Avito"},
    {"role": "assistant", "content": '{"is_lead": false, "confidence": 0.97, "reason": "вакансия риэлтора, не отклики с вакансий как база"}'},
    {"role": "user", "content": "[Триггер: 'офферы [оффер]']\nneeds a large number of Indian customers Large daily demand for USDT 1 USDT = 130-150 Indian Rupees"},
    {"role": "assistant", "content": '{"is_lead": false, "confidence": 0.99, "reason": "USDT обмен крипта, не связано с нашими продуктами"}'},
    {"role": "user", "content": "[Триггер: 'контакты/база контактов [контакт]']\nToolsy Market подписки на Ai сервисы CHATGPT CLAUDE GEMINI CURSOR цены ниже официальных"},
    {"role": "assistant", "content": '{"is_lead": false, "confidence": 0.98, "reason": "продажа подписок на AI сервисы, не наш продукт"}'},
    {"role": "user", "content": "[Триггер: 'проблемы с трафиком']\nпроблемы с траффиком сейчас пиздец"},
    {"role": "assistant", "content": '{"is_lead": true, "confidence": 0.95, "reason": "человек с проблемой трафика — потенциальный клиент"}'},
    {"role": "user", "content": "[Триггер: 'нет лидов']\nнет лидов вообще база тухлая"},
    {"role": "assistant", "content": '{"is_lead": true, "confidence": 0.96, "reason": "жалоба на отсутствие лидов — можно предложить базу"}'},
    {"role": "user", "content": "[Триггер: 'парсинг/парсер [парсинг]']\nРАССЫЛКА ПО ЧАТАМ 24 часа рассылки 10$ целевые чаты под ваш проект"},
    {"role": "assistant", "content": '{"is_lead": false, "confidence": 0.97, "reason": "услуга рассылки по чатам, не продажа баз/лидов"}'},
]


async def classify_lead(text: str, trigger: str, api_key: str = "", model: str = "") -> dict:
    """Classify if a message is a real lead or false positive using RouterAI."""
    if not api_key:
        return {"is_lead": True, "confidence": 1.0, "reason": "RouterAI disabled"}

    # Build messages: system prompt + few-shot examples + actual message
    messages = [{"role": "system", "content": _CLASSIFY_SYSTEM}]
    messages.extend(_CLASSIFY_EXAMPLES)
    messages.append({"role": "user", "content": f"[Триггер: '{trigger}']\n{text[:2000]}"})

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://routerai.ru/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model or "deepseek/deepseek-v4.1-flash",
                    "messages": messages,
                    "temperature": 0.1,
                    "max_tokens": 300,
                },
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                data = await resp.json()

        logger_ai.info("RouterAI full response: %s", str(data)[:800])

        if "choices" not in data:
            logger_ai.error("RouterAI: no 'choices' in response: %s", str(data)[:300])
            return {"is_lead": True, "confidence": 0.5, "reason": f"API error: {data.get('error', {}).get('message', 'unknown')}"}

        choice = data["choices"][0]
        finish_reason = choice.get("finish_reason", "unknown")
        content = choice["message"]["content"].strip()
        logger_ai.info("RouterAI raw content (finish=%s): %s", finish_reason, content[:500])

        if not content:
            logger_ai.warning("RouterAI: empty response content")
            return {"is_lead": True, "confidence": 0.5, "reason": "Empty AI response"}

        # Strip markdown code fences if present
        cleaned = re.sub(r'```(?:json)?\s*', '', content)
        cleaned = re.sub(r'```\s*$', '', cleaned).strip()

        # Try multiple strategies to extract valid JSON
        result = None

        # Strategy 1: find { ... } with balanced braces (handles nested quotes)
        brace_start = cleaned.find('{')
        brace_end = cleaned.rfind('}')
        if brace_start != -1 and brace_end > brace_start:
            candidate = cleaned[brace_start:brace_end + 1]
            try:
                result = json.loads(candidate)
            except json.JSONDecodeError:
                pass

        # Strategy 2: fix common issues - single quotes, trailing commas
        if result is None and brace_start != -1 and brace_end > brace_start:
            candidate = cleaned[brace_start:brace_end + 1]
            candidate = candidate.replace("'", '"')
            candidate = re.sub(r',\s*}', '}', candidate)
            candidate = re.sub(r',\s*]', ']', candidate)
            try:
                result = json.loads(candidate)
            except json.JSONDecodeError:
                pass

        # Strategy 3: regex extract key-value pairs manually
        if result is None:
            is_lead_match = re.search(r'"?is_lead"?\s*:\s*(true|false)', cleaned, re.I)
            conf_match = re.search(r'"?confidence"?\s*:\s*([\d.]+)', cleaned)
            reason_match = re.search(r'"?reason"?\s*:\s*"([^"]*)"', cleaned)
            if is_lead_match:
                result = {
                    "is_lead": is_lead_match.group(1).lower() == "true",
                    "confidence": float(conf_match.group(1)) if conf_match else 0.5,
                    "reason": reason_match.group(1) if reason_match else "",
                }

        if result is not None:
            return {
                "is_lead": bool(result.get("is_lead", True)),
                "confidence": float(result.get("confidence", 0.5)),
                "reason": str(result.get("reason", "")),
            }

        logger_ai.warning("RouterAI: could not parse JSON from response: %s", cleaned[:300])
        return {"is_lead": True, "confidence": 0.5, "reason": "Parse error, defaulting to lead"}

    except asyncio.TimeoutError:
        logger_ai.warning("RouterAI: timeout")
        return {"is_lead": True, "confidence": 0.5, "reason": "Timeout, defaulting to lead"}
    except Exception as e:
        logger_ai.error("RouterAI error: %s", e)
        return {"is_lead": True, "confidence": 0.5, "reason": f"Error: {e}"}


class LeadMonitor:
    def __init__(self, account_cfg: dict, global_cfg: dict,
                 registry: Optional[ContactRegistry] = None):
        self.name = account_cfg.get("name", "default")
        self.logger = logging.getLogger(f"lead_monitor.{self.name}")
        self.account_cfg = account_cfg
        self.global_cfg = global_cfg
        self.registry = registry or ContactRegistry(global_cfg.get("registry_file", "lead_registry.json"))

        # Per-account settings
        api_id = account_cfg["api_id"]
        api_hash = account_cfg["api_hash"]
        session_name = account_cfg.get("session_name", f"session_{self.name}")
        session_string = account_cfg.get("session_string") or global_cfg.get("session_string", "")
        bot_token = account_cfg["bot_token"]
        phone = account_cfg.get("phone", "")
        target_chat_id = account_cfg.get("target_chat_id", 0)
        keywords_file = account_cfg.get("keywords_file", "keywords.txt")
        chats_file = account_cfg.get("chats_file", os.environ.get("CHATS_FILE", "chats.txt"))
        state_file = account_cfg.get("state_file", f"state_{self.name}.json")
        dedup_ttl = global_cfg.get("dedup_ttl", 1800)

        # AI config (shared across accounts)
        self.routerai_api_key = global_cfg.get("routerai_api_key", "")
        self.routerai_model = global_cfg.get("routerai_model", "deepseek/deepseek-v4.1-flash")
        self.routerai_enabled = bool(self.routerai_api_key)

        self.phone = phone
        self.headless = global_cfg.get("headless", True)
        self.session_name = session_name
        self.session_string = str(session_string or "").strip()
        self.api_id = api_id
        self.api_hash = api_hash
        self.chat_filter_configured = Path(chats_file).exists()
        self.chat_filters = load_lines(chats_file)
        self.chat_filter_ids = {int(x) for x in self.chat_filters if x.lstrip("-").isdigit()}
        self.chat_filter_usernames = {x.lstrip("@").lower() for x in self.chat_filters if not x.lstrip("-").isdigit()}
        allow_all = account_cfg.get("allow_all_chats", global_cfg.get("allow_all_chats", False))
        self.allow_all_chats = str(allow_all).lower() in {"1", "true", "yes", "on"} if isinstance(allow_all, str) else bool(allow_all)
        # Empty or missing chats.txt means monitor nothing unless the operator
        # explicitly opts into every dialog with allow_all_chats.
        self.chat_filter_configured = not self.allow_all_chats

        self.state = State(state_file, default_target=target_chat_id)
        if not self.state.outreach_template:
            self.state.outreach_template = account_cfg.get("outreach_template") or global_cfg.get("outreach_template", "")
        if not self.state.keywords:
            fk = load_lines(keywords_file)
            if fk:
                self.state.keywords = fk
                self.logger.info("Loaded %d keywords from %s", len(fk), keywords_file)
        self.pat = build_pattern(self.state.keywords)
        self.custom_pats = build_custom_patterns(self.state.keywords)
        self.cache: TTLCache = TTLCache(maxsize=10_000, ttl=dedup_ttl)
        operator_ids = account_cfg.get("operator_user_ids", global_cfg.get("operator_user_ids", []))
        self.bot = BotAPI(bot_token, self.state, account_name=self.name,
                          registry=self.registry, operator_user_ids=operator_ids)
        self.bot.mini_app_url = account_cfg.get("webapp_url") or global_cfg.get("webapp_url", "")
        self.bot._send_callback = self._send_outreach
        self.bot._patterns_refresh_callback = self._refresh_patterns
        self.client = self._new_client()
        self.watched: set[int] = set()
        self._me_id: Optional[int] = None
        self._waiting_auth = False
        self._auth_event = asyncio.Event()
        self._auth_code: Optional[str] = None
        # Rate limiter: max 20 alerts per 60 seconds
        self._alert_times: list[float] = []
        self._alert_rate_limit = 20
        self._alert_rate_window = 60
        # Sender cache to avoid repeated get_sender() calls
        self._sender_cache: TTLCache = TTLCache(maxsize=5_000, ttl=3600)

    def _refresh_patterns(self):
        """Rebuild keyword patterns after an operator changes keywords."""
        self.pat = build_pattern(self.state.keywords)
        self.custom_pats = build_custom_patterns(self.state.keywords)

    def _new_client(self):
        if self.session_string:
            return TelegramClient(StringSession(self.session_string), self.api_id, self.api_hash)
        return TelegramClient(self.session_name, self.api_id, self.api_hash)

    async def _check_session(self) -> bool:
        """Check if existing session is valid."""
        sess = bool(self.session_string) or Path(f"{self.session_name}.session").exists()
        if not sess:
            return False
        try:
            await self.client.connect()
            if await self.client.is_user_authorized():
                self.logger.info("Session valid, authorized"); return True
            self.logger.warning("Session not authorized, removing")
            await self.client.disconnect()
            p = Path(f"{self.session_name}.session")
            if not self.session_string and p.exists(): p.unlink()
            # Recreate client so it's clean for future auth
            self.client = self._new_client()
        except Exception as e:
            # Network/DNS/Telegram outages do not prove that the on-disk
            # session is invalid.  Keep it for the next reconnect attempt.
            self.logger.warning("Session check failed transiently (%s); keeping session", e)
            try: await self.client.disconnect()
            except Exception: pass
            # Recreate client
            self.client = self._new_client()
        return False

    async def _do_auth(self, chat_id: int):
        """Explain local StringSession setup; never collect codes or 2FA in a bot."""
        if await self._check_session():
            await self.bot.send(chat_id, "✅ Уже авторизован. Мониторинг продолжает работу.")
            return
        await self.bot.send(
            chat_id,
            "🔐 Авторизация через Telegram-бот отключена: коды и пароль 2FA нельзя передавать в чат.\n\n"
            "Сгенерируйте Telethon StringSession локально (скрипт gen_session.py), "
            "добавьте SESSION_STRING в переменные окружения Bothost и перезапустите сервис."
        )

    async def _post_auth(self):
        """Called after successful authorization."""
        self.state.cleanup_old_hashes()
        await self.discover_chats()
        self.client.add_event_handler(self.on_msg, events.NewMessage())
        self.logger.info("[%s] Listening %d chats, target=%s, blacklisted=%d",
                    self.name, len(self.watched), self.state.target, len(self.state.blacklisted_chats))

    @staticmethod
    def _entity_chat_ids(entity) -> set[int]:
        """Return Telethon's possible IDs for one dialog entity.

        ``entity.id`` is positive while channel events use the marked
        ``-100...`` form.  Keeping all aliases makes chats.txt work with IDs
        copied from either Telethon logs or Telegram links.
        """
        try:
            eid = int(entity.id)
        except (AttributeError, TypeError, ValueError):
            return set()
        ids = {eid, -eid}
        if isinstance(entity, Channel):
            ids.add(-1000000000000 - eid)
        return ids

    def _chat_allowed(self, entity) -> bool:
        if self.allow_all_chats:
            return True
        if not self.chat_filter_ids and not self.chat_filter_usernames:
            return False
        username = (getattr(entity, "username", None) or "").lower()
        return bool(username and username in self.chat_filter_usernames) or bool(
            self._entity_chat_ids(entity) & self.chat_filter_ids
        )

    async def discover_chats(self):
        me = await self.client.get_me(); self._me_id = me.id; count = 0; names = []
        self.logger.info("[%s] Logged in as: %s (ID: %d)", self.name, me.first_name, me.id)
        async for d in self.client.iter_dialogs():
            if d.is_user: continue
            e = d.entity
            if getattr(e,'id',None)==me.id: continue
            if isinstance(e,(Channel,TGChat)):
                if not self._chat_allowed(e):
                    continue
                self.watched.update(self._entity_chat_ids(e))
                t=getattr(e,'title','?'); u=getattr(e,'username',None)
                name = f"{t} (@{u})" if u else t
                # Show the event-compatible marked ID for channels.
                display_id = (-1000000000000 - int(e.id)) if isinstance(e, Channel) else -int(e.id)
                names.append((name, display_id)); count+=1
        self.bot._chats_cache=names
        if not self.watched:
            self.logger.error("[%s] No chats!", self.name)
            return  # Don't sys.exit — other accounts may still work
        self.logger.info("[%s] Discovered %d chats", self.name, count)

    async def _check_rate_limit(self) -> bool:
        """Returns True if rate limited (should skip alert)."""
        now = time.time()
        # Remove old entries outside window
        self._alert_times = [t for t in self._alert_times if now - t < self._alert_rate_window]
        if len(self._alert_times) >= self._alert_rate_limit:
            self.logger.warning("[%s] RATE LIMITED: %d alerts in %ds, skipping",
                          self.name, len(self._alert_times), self._alert_rate_window)
            return True
        self._alert_times.append(now)
        return False

    def is_dup(self, chat_id: int, message_id: int, text: str) -> bool:
        """Check duplicate using message_id (primary) + text hash (fallback)."""
        # Primary: unique per message
        msg_key = msg_dedup_key(chat_id, message_id)
        if self.state.is_sent(msg_key):
            return True
        # Secondary: text-based (catches forwarded duplicates)
        txt_key = dedup_key(chat_id, text)
        if txt_key in self.cache or self.state.is_sent(txt_key):
            return True
        # Mark both
        self.state.mark_sent(msg_key)
        self.cache[txt_key] = True
        self.state.mark_sent(txt_key)
        return False

    def find_trig(self, nt: str) -> Optional[str]:
        return find_trigger(nt, self.pat, self.custom_pats)

    async def _send_outreach(self, lead_id: str, message_override: str = "") -> tuple[bool, str]:
        """Send only after an explicit operator confirmation in the bot/Mini App."""
        lead = self.state.get_lead(lead_id)
        if not lead:
            return False, "Лид не найден"
        user_id = int(lead.get("user_id") or 0)
        if not user_id:
            return False, "У пользователя нет доступного Telegram ID"
        if lead.get("status") != "review":
            return False, "Сначала откройте предпросмотр и подтвердите отправку"
        registry_record = self.registry.get(user_id)
        if registry_record and registry_record.get("status") in {
            "contacted", "blocked", "skipped", "cancelled", "sending", "failed"
        }:
            return False, "Для этого пользователя отправка закрыта"
        # Bot confirmation uses the immutable preview captured by ``offer``.
        # Mini App may provide an explicit edited message, which its action
        # handler snapshots before reaching this function.
        text = (message_override or lead.get("preview_text") or "").strip()[:4000]
        if not text:
            return False, "Нет подтверждённого текста сообщения"
        text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        claimed, claim_reason = self.registry.claim_send(user_id, lead_id, text_hash, self.name)
        if not claimed:
            return False, claim_reason
        self.state.update_lead(lead_id, status="sending", send_started_at=int(time.time()),
                               preview_text=text, preview_hash=text_hash)
        try:
            await self.client.send_message(user_id, text)
        except FloodWaitError as e:
            error = f"Telegram попросил подождать {e.seconds} сек."
            # Treat every failed attempt as terminal.  The result may be
            # ambiguous to the caller, so a retry cannot send a duplicate.
            self.state.update_lead(lead_id, status="failed", last_error=error, retry_blocked=True)
            self.registry.update(user_id, "failed", lead_id=lead_id,
                                 last_error=error, retry_blocked=True)
            return False, error
        except Exception as e:
            self.logger.warning("[%s] outreach failed user=%s: %s", self.name, user_id, e)
            error = str(e)[:500]
            self.state.update_lead(lead_id, status="failed", last_error=error, retry_blocked=True)
            self.registry.update(user_id, "failed", lead_id=lead_id,
                                 last_error=error, retry_blocked=True)
            return False, f"Telegram не подтвердил сообщение: {error}"

        self.state.update_lead(lead_id, status="contacted", contacted_at=int(time.time()),
                               sent_text=text, template=text)
        self.registry.update(user_id, "contacted", lead_id=lead_id,
                             account=self.name, contacted_at=int(time.time()))
        self.state.data["stats"]["contacted"] = self.state.data["stats"].get("contacted", 0) + 1
        self.state.save()
        self.logger.info("[%s] OUTREACH_SENT user=%s lead=%s", self.name, user_id, lead_id)
        return True, "Сообщение отправлено"

    async def on_msg(self, ev):
        try:
            if isinstance(ev.message, MessageService):
                return

            # ── Фильтр личек: пропускаем только группы и каналы ──────────────
            chat = await ev.get_chat()
            if isinstance(chat, User):
                # The recipient replies to the Telethon userbot, not to the
                # Bot API control bot.  Honour the same opt-out in this path.
                dm_text = (ev.raw_text or "").strip().casefold()
                if not ev.out and dm_text in {"/stop", "/unsubscribe", "stop", "unsubscribe", "стоп", "отписка"}:
                    sender = await ev.get_sender()
                    optout_id = int(getattr(sender, "id", 0) or getattr(chat, "id", 0) or 0)
                    if optout_id:
                        self.registry.update(optout_id, "blocked", reason="user_opt_out")
                        self.logger.info("[%s] DM opt-out user=%s", self.name, optout_id)
                self.logger.debug("[%s] SKIP DM: chat=%s", self.name, ev.chat_id)
                return

            # ── Чёрный список чатов ───────────────────────────────────────────
            if self.state.is_blacklisted(ev.chat_id):
                self.logger.debug("[%s] SKIP blacklisted chat: %s", self.name, ev.chat_id)
                return

            sender = await ev.get_sender()
            if isinstance(sender, User) and sender.bot:
                self.logger.debug("[%s] SKIP bot: chat=%s sender=%s", self.name, ev.chat_id, sender.id)
                return
            raw = ev.raw_text or ""
            if not raw.strip():
                return

            is_self = isinstance(sender, User) and sender.id == self._me_id
            self.logger.info("[%s] MSG|chat=%s|sender=%s|out=%s|self=%s|text='%s'",
                        self.name, ev.chat_id, getattr(sender,'id','?'), ev.out, is_self, raw[:80])
            if is_self:
                return

            # Process only chats explicitly selected in chats.txt. The event
            # handler is global, so silently ignore every other dialog.
            if ev.chat_id not in self.watched:
                self.logger.debug("[%s] SKIP chat not in chats.txt: %s", self.name, ev.chat_id)
                return

            trig = self.find_trig(normalize(raw))
            if trig is None:
                self.logger.debug("[%s] NO TRIGGER in: '%s'", self.name, raw[:60])
                return
            if self.is_dup(ev.chat_id, ev.id, raw):
                self.logger.debug("[%s] DUP: chat=%s msg=%s", self.name, ev.chat_id, ev.id)
                return

            ct = getattr(chat, "title", None) or getattr(chat, "first_name", "?")
            cu = getattr(chat, "username", None)
            if isinstance(sender, User):
                sn = f"{sender.first_name or ''} {sender.last_name or ''}".strip() or "?"
                su, sid = sender.username, sender.id
            else:
                sn, su, sid = str(sender) if sender else "?", None, 0

            lnk = msg_link(chat, ev.id)

            # Ссылка на сам чат (если есть username — публичная, иначе tg://openmessage)
            if cu:
                chat_lnk = f"https://t.me/{cu}"
            else:
                chat_lnk = f"https://t.me/c/{abs(ev.chat_id)}/1"

            self.logger.info("[%s] LEAD|chat=%s (id=%s)|trigger='%s'|sender=%s", self.name, ct, ev.chat_id, trig, sn)

            # AI classification (if enabled)
            ai_info = ""
            if self.routerai_enabled:
                try:
                    classification = await classify_lead(raw, trig, self.routerai_api_key, self.routerai_model)
                    self.logger.info("[%s] AI_CLASSIFY|is_lead=%s|confidence=%.2f|reason=%s",
                               self.name, classification["is_lead"], classification["confidence"], classification["reason"])
                    # Filter if confident it's NOT a lead (threshold 0.7)
                    if not classification["is_lead"] and classification["confidence"] >= 0.7:
                        self.logger.info("[%s] FALSE_POSITIVE filtered by AI (conf=%.2f): '%s'",
                                   self.name, classification["confidence"], raw[:60])
                        self.state.inc_ai_filtered()
                        return
                    self.state.inc_ai_passed()
                    ai_info = f"\n\n🤖 <i>AI: {'✅ Лид' if classification['is_lead'] else '⚠️ Сомнительно'} ({classification['confidence']:.0%}) — {html.escape(classification['reason'])}</i>"
                except Exception as e:
                    self.logger.error("[%s] AI classification failed: %s", self.name, e)
                    # Continue without AI filtering on error

            # Rate limit check before sending
            if await self._check_rate_limit():
                self.logger.warning("[%s] RATE LIMITED, skipping alert for: '%s'", self.name, raw[:60])
                return

            # One candidate per Telegram user across all accounts. Usernames can
            # change, therefore the registry key is the numeric user id.
            if not sid:
                self.logger.info("[%s] SKIP lead without a user id", self.name)
                return
            lead_id = hashlib.sha1(f"{self.name}:{ev.chat_id}:{ev.id}".encode()).hexdigest()[:16]
            if not self.registry.register_candidate(sid, lead_id, self.name):
                self.logger.info("[%s] SUPPRESSED user=%s (already offered/contacted)", self.name, sid)
                return

            first_name = sender.first_name if isinstance(sender, User) else ""
            last_name = sender.last_name if isinstance(sender, User) else ""
            lead = {
                "lead_id": lead_id,
                "status": "new",
                "created_at": int(time.time()),
                "updated_at": int(time.time()),
                "account": self.name,
                "user_id": sid,
                "sender": {
                    "first_name": first_name or "",
                    "last_name": last_name or "",
                    "username": su or "",
                    "display_name": sn,
                },
                "chat_id": ev.chat_id,
                "chat_title": ct,
                "chat_username": cu or "",
                "message_id": ev.id,
                "message": raw[:4000],
                "trigger": trig,
                "source_url": lnk,
                "ai": classification if self.routerai_enabled and 'classification' in locals() else None,
            }
            self.state.add_lead(lead_id, lead)

            cte = html.escape(ct)
            if cu:
                cte += f" (@{html.escape(cu)})"
            up = f"@{html.escape(su)} / " if su else ""

            body = (
                f"\U0001f6a8 <b>Лид! [{self.name}]</b>\n"
                f"🟡 <b>Статус:</b> ждёт решения\n\n"
                f"\U0001f4cc <b>Обнаружен в:</b> <a href=\"{chat_lnk}\">{cte}</a>  <code>[ID: {ev.chat_id}]</code>\n"
                f"\U0001f464 <b>Отправитель:</b> {html.escape(sn)} ({up}ID:<code>{sid}</code>)\n"
                f"\U0001f511 <b>Триггер:</b> <code>{html.escape(trig)}</code>\n\n"
                f"<blockquote>{html.escape(raw)[:3900]}</blockquote>\n\n"
                f'<a href="{lnk}">➡️ Перейти к сообщению</a>'
                f'{ai_info}'
            )

            t = self.state.target
            if t:
                mid = await self.bot.send_alert(t, body, self.state.thread_id, lead_id=lead_id)
                if mid:
                    self.state.inc_triggers()
            else:
                self.logger.warning("[%s] No target! Use /target", self.name)
        except FloodWaitError as e:
            await asyncio.sleep(e.seconds)
        except ConnectionError:
            self.logger.error("[%s] Disconnected", self.name)
        except Exception:
            self.logger.exception("[%s] Error", self.name)

    async def run(self):
        self.logger.info("[%s] Starting v3...", self.name)
        # Wire up bot callbacks
        self.bot._auth_trigger = lambda cid: self._do_auth(cid)
        async def _status():
            auth = await self._check_session() if not self.watched else True
            check = "\u2705" if auth and self.watched else "\u274c"
            return (f"\U0001f4ca <b>Status [{self.name}]</b>\n\n"
                    f"Auth: {check}\n"
                    f"Chats: {len(self.watched)}\n"
                    f"Target: <code>{self.state.target}</code>\n"
                    f"Keywords: {len(self.state.keywords)}")
        self.bot._status_callback = _status

        # Start bot polling FIRST (works without Telethon auth)
        poll_task = asyncio.create_task(self.bot.poll())

        # Check existing session once
        session_valid = await self._check_session()
        if session_valid:
            await self._post_auth()
            try:
                await self.client.run_until_disconnected()
            except KeyboardInterrupt:
                pass
            finally:
                self.bot.stop(); poll_task.cancel()
                try: await poll_task
                except asyncio.CancelledError: pass
                await self.bot.close()
        else:
            self.logger.info("[%s] No valid session. Waiting for /auth command via bot...", self.name)
            target = self.state.target
            if target:
                await self.bot.send(target,
                    f"\u26a0\ufe0f <b>Telethon not authorized! [{self.name}]</b>\n\n"
                    "Generate a StringSession locally and set SESSION_STRING before restarting.\n"
                    "Use /auth for the local setup instructions.")
            # Keep running bot polling, wait forever
            try:
                while True:
                    await asyncio.sleep(3600)
            except KeyboardInterrupt:
                pass
            finally:
                self.bot.stop(); poll_task.cancel()
                try: await poll_task
                except asyncio.CancelledError: pass
                await self.bot.close()


class MiniAppServer:
    """Optional API/static server for the Telegram Mini App inbox."""

    def __init__(self, workers: list[LeadMonitor], cfg: dict):
        self.workers = workers
        self.cfg = cfg
        self.host = cfg.get("webapp_host", "127.0.0.1")
        self.port = int(cfg.get("webapp_port", 8080))
        self.allow_unauth = bool(cfg.get("webapp_allow_unauth", False))
        self.operator_user_ids = {int(x) for x in cfg.get("operator_user_ids", []) if str(x).isdigit()}
        self.root = Path(__file__).parent / "miniapp"
        self.runner: Optional[web.AppRunner] = None

    def _find_lead(self, lead_id: str):
        for worker in self.workers:
            lead = worker.state.get_lead(lead_id)
            if lead:
                return worker, lead
        return None, None

    def _serialize_lead(self, lead: dict, worker: LeadMonitor) -> dict:
        sender = lead.get("sender") or {}
        status = lead.get("status", "new")
        ui_status = {"contacted": "sent", "blocked": "suppressed"}.get(status, status)
        ai = lead.get("ai") or {}
        created = lead.get("created_at")
        created_label = time.strftime("%d.%m %H:%M", time.localtime(created)) if created else ""
        return {
            **lead,
            "id": lead.get("lead_id"),
            "status": ui_status,
            "source": lead.get("chat_title", "Telegram-чат"),
            "source_handle": f"@{lead.get('chat_username')}" if lead.get("chat_username") else "",
            "name": sender.get("display_name") or "Без имени",
            "username": sender.get("username") or "",
            "initials": "".join(part[:1] for part in (sender.get("display_name") or "?").split())[:2].upper(),
            "topic": lead.get("trigger", "сигнал"),
            "score": round(float(ai.get("confidence", 0)) * 100) if ai else 0,
            "created_at": created_label,
            "created_at_ts": created or 0,
            "account": worker.name,
        }

    @staticmethod
    def _is_loopback(request: web.Request) -> bool:
        return (request.remote or "").strip("[]").lower() in {"127.0.0.1", "::1", "localhost"}

    async def _auth(self, request: web.Request, worker: Optional[LeadMonitor] = None,
                    write: bool = False) -> dict:
        if self.allow_unauth:
            # Development escape hatch: local reads only. Never expose a
            # writable unauthenticated endpoint on a VPS.
            if write or not self._is_loopback(request):
                raise web.HTTPForbidden(text="Unauthenticated access is local read-only only")
            return {"id": 0, "username": "local"}
        init_data = request.headers.get("X-Telegram-Init-Data", "")
        candidates = [worker] if worker else self.workers
        for candidate in candidates:
            user = validate_webapp_init_data(init_data, candidate.bot.token)
            if user:
                user_id = int(user.get("id") or 0)
                allowed = candidate.bot.operator_user_ids
                if allowed and user_id in allowed:
                    return user
                raise web.HTTPForbidden(text="Operator access required")
        raise web.HTTPUnauthorized(text="Invalid Telegram Mini App session")

    async def index(self, request: web.Request):
        path = self.root / "index.html"
        if not path.exists():
            raise web.HTTPNotFound(text="Mini App is not installed")
        return web.FileResponse(path)

    async def asset(self, request: web.Request):
        root = self.root.resolve()
        path = (root / request.match_info.get("path", "")).resolve()
        if root not in path.parents and path != root:
            raise web.HTTPForbidden()
        if not path.exists() or not path.is_file():
            raise web.HTTPNotFound()
        return web.FileResponse(path)

    async def api_leads(self, request: web.Request):
        await self._auth(request, write=False)
        status = request.query.get("status", request.query.get("filter", ""))
        if status == "all":
            status = ""
        all_items = []
        for worker in self.workers:
            for lead in worker.state.list_leads(""):
                all_items.append(self._serialize_lead(lead, worker))
        all_items.sort(key=lambda x: x.get("created_at_ts", 0), reverse=True)
        result = [item for item in all_items if not status or item.get("status") == status]
        stats = {
            "new": sum(1 for item in all_items if item.get("status") == "new"),
            "review": sum(1 for item in all_items if item.get("status") == "review"),
            "sent": sum(1 for item in all_items if item.get("status") == "sent"),
        }
        try:
            limit = max(1, min(200, int(request.query.get("limit", "50"))))
        except ValueError:
            limit = 50
        return web.json_response({"leads": result[:limit], "stats": stats})

    async def api_lead(self, request: web.Request):
        worker, lead = self._find_lead(request.match_info["lead_id"])
        if not lead:
            raise web.HTTPNotFound(text="Lead not found")
        await self._auth(request, worker=worker, write=False)
        return web.json_response(self._serialize_lead(lead, worker))

    async def api_settings(self, request: web.Request):
        await self._auth(request, write=request.method != "GET")
        worker = self.workers[0] if self.workers else None
        if not worker:
            raise web.HTTPServiceUnavailable(text="No workers")
        if request.method == "GET":
            return web.json_response({"mode": worker.state.outreach_mode,
                                      "template": worker.state.outreach_template or worker.global_cfg.get("outreach_template", "")})
        try:
            payload = await request.json()
        except Exception:
            raise web.HTTPBadRequest(text="Expected JSON body")
        if "mode" in payload:
            for item in self.workers:
                item.state.outreach_mode = str(payload["mode"])
        if "template" in payload:
            for item in self.workers:
                item.state.outreach_template = str(payload["template"])
        return web.json_response({"mode": worker.state.outreach_mode, "template": worker.state.outreach_template})

    async def api_action(self, request: web.Request):
        lead_id = request.match_info["lead_id"]
        worker, lead = self._find_lead(lead_id)
        if not worker or not lead:
            raise web.HTTPNotFound(text="Lead not found")
        await self._auth(request, worker=worker, write=True)
        try:
            payload = await request.json()
        except Exception:
            raise web.HTTPBadRequest(text="Expected JSON body")
        route_action = {"propose": "offer", "skip": "skip", "suppress": "block", "send": "send"}.get(
            request.match_info.get("action", ""), ""
        )
        action = str(payload.get("action") or route_action).lower()
        if action == "offer":
            if lead.get("status") in {"blocked", "contacted", "skipped", "cancelled", "sending", "failed"}:
                return web.json_response({"ok": False, "message": "Для этого пользователя отправка закрыта"}, status=409)
            template = worker.state.outreach_template or worker.global_cfg.get("outreach_template", "")
            if not template:
                template = "Здравствуйте, {first_name}! Вы писали про «{trigger}» в чате «{chat_title}». Если актуально, могу помочь. Если сообщения не нужны, напишите «стоп»."
            preview = render_outreach_template(template, lead)
            worker.state.update_lead(
                lead_id, status="review", offered_at=int(time.time()), preview_text=preview,
                preview_hash=hashlib.sha256(preview.encode()).hexdigest()
            )
            worker.registry.update(int(lead.get("user_id") or 0), "review", lead_id=lead_id)
            return web.json_response({"ok": True, "status": "review", "preview": preview})
        if action == "skip":
            worker.state.update_lead(lead_id, status="skipped", skipped_at=int(time.time()))
            worker.registry.update(int(lead.get("user_id") or 0), "skipped", lead_id=lead_id)
            return web.json_response({"ok": True, "status": "skipped"})
        if action == "block":
            worker.state.update_lead(lead_id, status="blocked", blocked_at=int(time.time()))
            worker.registry.update(int(lead.get("user_id") or 0), "blocked", lead_id=lead_id)
            return web.json_response({"ok": True, "status": "blocked"})
        if action == "send":
            message = str(payload.get("message", "")).strip()[:4000]
            # Mini App's composer is itself the explicit confirmation step.
            # Snapshot its exact text before the asynchronous Telegram call.
            current = worker.state.get_lead(lead_id) or {}
            if current.get("status") == "new":
                worker.state.update_lead(lead_id, status="review", offered_at=int(time.time()))
                worker.registry.update(int(current.get("user_id") or 0), "review", lead_id=lead_id)
            if message:
                worker.state.update_lead(
                    lead_id, preview_text=message,
                    preview_hash=hashlib.sha256(message.encode()).hexdigest()
                )
            ok, message = await worker._send_outreach(lead_id, message)
            return web.json_response({"ok": ok, "status": "contacted" if ok else "failed", "message": message}, status=200 if ok else 409)
        raise web.HTTPBadRequest(text="Unknown action")

    async def start(self):
        if not self.root.exists():
            logger_web.warning("Mini App directory is missing: %s", self.root)
            return
        app = web.Application()
        app.router.add_get("/", self.index)
        app.router.add_get("/api/leads", self.api_leads)
        app.router.add_get("/api/leads/{lead_id}", self.api_lead)
        app.router.add_post("/api/leads/{lead_id}/action", self.api_action)
        app.router.add_get("/api/settings", self.api_settings)
        app.router.add_patch("/api/settings", self.api_settings)
        # Friendly action aliases used by the Mini App UI.
        app.router.add_post("/api/leads/{lead_id}/{action}", self.api_action_alias)
        app.router.add_get("/{path:.*}", self.asset)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        try:
            site = web.TCPSite(self.runner, self.host, self.port)
            await site.start()
            logger_web.info("Mini App server listening on http://%s:%d", self.host, self.port)
        except OSError as exc:
            logger_web.error("Mini App server failed on %s:%d: %s", self.host, self.port, exc)
            await self.runner.cleanup()
            self.runner = None

    async def api_action_alias(self, request: web.Request):
        # Keep one authorization and state transition implementation. The
        # route name supplies the action; a JSON body may still override it.
        return await self.api_action(request)

    async def stop(self):
        if self.runner:
            await self.runner.cleanup()
            self.runner = None


async def main():
    """Load config and run all accounts in parallel."""
    cfg = load_config()
    accounts = cfg.get("accounts", [])
    if not accounts:
        logger_cfg.error("No accounts configured! Check config.json")
        return

    logger_cfg.info("Starting %d account(s)...", len(accounts))

    # Filter out accounts with missing credentials
    valid_accounts = []
    for acc in accounts:
        name = acc.get("name", "?")
        if not acc.get("api_id") or not acc.get("api_hash"):
            logger_cfg.warning("Account '%s': missing api_id/api_hash, skipping", name)
            continue
        if not acc.get("bot_token"):
            logger_cfg.warning("Account '%s': missing bot_token, skipping", name)
            continue
        valid_accounts.append(acc)

    if not valid_accounts:
        logger_cfg.error("No valid accounts to start!")
        return

    # One suppression registry is shared by every account. This prevents the
    # same user from appearing again when several monitoring accounts see them.
    registry = ContactRegistry(cfg.get("registry_file", "lead_registry.json"))
    workers = [LeadMonitor(acc, cfg, registry=registry) for acc in valid_accounts]
    miniapp = MiniAppServer(workers, cfg)
    await miniapp.start()

    # Run all in parallel
    tasks = [asyncio.create_task(w.run()) for w in workers]
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger_cfg.info("Shutting down...")
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await miniapp.stop()


if __name__=="__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: logger_cfg.info("Stopped")
