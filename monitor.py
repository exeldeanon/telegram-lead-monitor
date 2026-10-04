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
    "a": "а", "b": "б", "v": "в", "g": "г", "d": "д",
    "z": "з", "j": "й", "k": "к", "l": "л", "m": "м",
    "n": "н", "p": "п", "r": "р", "s": "с", "t": "т",
    "u": "у", "f": "ф", "h": "х", "w": "в", "q": "к",
    "A": "а", "B": "б", "V": "в", "G": "г", "D": "д",
    "Z": "з", "J": "й", "K": "к", "L": "л", "M": "м",
    "N": "н", "P": "п", "R": "р", "S": "с", "T": "т",
    "U": "у", "F": "ф", "H": "х", "W": "в", "Q": "к",
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
     r"\bли[дт][ыауе]?\b"),
    ("горячие лиды",
     r"\b(горяч\w*\s+ли[дт]|ли[дт]\w*\s+горяч)\w*\b"),
    ("купить/нужны лиды",
     r"\b(куп\w+|нужн\w+|ищ\w+|продам|продаю|отдам|есть|слив\w+)\s+\w{0,10}\s*ли[дт]\w*\b"),
    ("база/базы",
     r"\b(баз[аыуе]|базы|базк\w+)\b"),
    ("купить базу",
     r"\b(куп\w+|нужн\w+|ищ\w+|где\s+взять|где\s+брать|продам|продаю)\s+\w{0,15}\s*(баз[аыуе]|базы)\b"),
    ("парсинг/парсер",
     r"\bпарс\w+\b"),
    ("отказники",
     r"\бот(каз|цеп|лип)\w*\b|\bотказник\w*\b"),
    ("контакты/база контактов",
     r"\bконтакт\w*\b|\bбаз[аы]\s+контакт\w*\b|\bищу\s+\w{0,10}\s*контакт\w*\b"),
    ("номера телефонов",
     r"\bномер[аыа]?\s+(телефон\w*|мобильн\w*|сотовых\w*)\b|\bтелефонн\w+\s+баз\w+\b|\bбаз[аы]\s+номер\w*\b|\bномера\s+для\s+обзвон\w*\b"),

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
     r"\b(бетт\w+|ставк\w+|букмек\w+|беттинг\w*|betting|sports?bet)\b"),
    ("казино/гемблинг",
     r"\b(казино|гемблинг|gambling|слот\w+|онлайн.казино)\b"),
    ("лиды для беттинга",
     r"\bли[дт]\w*\s+\w{0,15}\s*(бетт\w+|ставк\w+|казино|гемблинг)\b|\b(бетт\w+|ставк\w+|казино)\w*\s+\w{0,15}\s*ли[дт]\w*\b"),

    # ── Арбитраж трафика ─────────────────────────────────────────────────────
    ("арбитраж",
     r"\bарбитраж\w*\b|\bарб\b"),
    ("трафик",
     r"\b(трафик|траф\b|тrafik)\w*\b"),
    ("мотивированный трафик",
     r"\b(мотив\w+)\s+\w{0,10}\s*(трафик|траф)\w*\b|\bмотив\w+\s+(юзер|install|инсталл)\w*\b"),
    ("офферы",
     r"\bоффер\w*\b|\bофер\w+\b"),
    ("партнёрка/CPA",
     r"\b(партнёрка|партнерка|cpa|cpа|аффилиат|affiliate)\w*\b"),

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
     r"\b(где\s+(купить|взять|брать|найти|достать)|посовет\w+|кто\s+(продаёт|продает|занимается|может))\s+\w{0,25}\s*(баз\w+|лид\w+|hr\b|рекрут\w+|мфо|трафик|персонал)\w*\b"),
    ("продам/слив данных",
     r"\b(продам|продаю|сливаю|слив\b|отдам|есть\s+в\s+наличии)\s+\w{0,15}\s*(баз\w+|лид\w+|данны)\w*\b"),
]

# Компилируем встроенные паттерны один раз
_COMPILED_BUILTIN: list[tuple[str, re.Pattern]] = [
    (name, re.compile(pat, re.IGNORECASE | re.UNICODE))
    for name, pat in _BUILTIN_SMART_PATTERNS
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
                logger.warning("Bad PATTERN '%s': %s", name, ex)
    return result


def find_trigger(normalized_text: str, plain_pat: re.Pattern,
                 custom_pats: list[tuple[str, re.Pattern]]) -> Optional[str]:
    """
    Ищет первый сработавший триггер в нормализованном тексте.
    Возвращает человекочитаемое название триггера или None.

    Порядок проверки:
      1. Встроенные смарт-кластеры (_COMPILED_BUILTIN)
      2. Пользовательские PATTERN: строки из keywords.txt
      3. Точные ключевые фразы из keywords.txt
    """
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
        self._chats_cache: list[tuple[str, int]] = []  # [(name, chat_id), ...]
        self._auth_callback = None  # async callable(code_str) for auth flow
        self._auth_trigger = None   # async callable(chat_id) to start auth
        self._status_callback = None  # async callable() -> str

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
                        logger.info("Chat migrated to %s, updating target", new_cid)
                        self.state.target = new_cid
                        # Retry with new chat_id
                        if "chat_id" in kw:
                            kw["chat_id"] = new_cid
                        async with s.post(url, json=kw, timeout=aiohttp.ClientTimeout(total=30)) as r2:
                            data = await r2.json()
                        return data
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

    async def send(self, chat_id: int, text: str, reply_markup=None):
        kw = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
        if reply_markup:
            kw["reply_markup"] = reply_markup
        await self.api("sendMessage", **kw)

    async def _handle_cmd(self, msg: dict):
        text = msg.get("text", "").strip()
        # Strip @BotName suffix from commands
        if "@" in text.split()[0]:
            text = text.split("@")[0] + (" " + " ".join(text.split()[1:]) if len(text.split()) > 1 else "")
        text = text.strip()
        cid = msg["chat"]["id"]
        if text in ("/start", "/help"):
            await self.send(cid,
                "\U0001f916 <b>Lead Monitor v2</b>\n\n"
                "/auth \u2014 \u0430\u0432\u0442\u043e\u0440\u0438\u0437\u0430\u0446\u0438\u044f Telethon\n"
                "/target <code>ID</code> \u2014 \u0447\u0430\u0442 \u0434\u043b\u044f \u0430\u043b\u0435\u0440\u0442\u043e\u0432\n"
                "/keywords \u2014 \u0442\u0440\u0438\u0433\u0433\u0435\u0440\u044b\n"
                "/addkeyword <code>\u0444\u0440\u0430\u0437\u0430</code>\n"
                "/delkeyword <code>\u0444\u0440\u0430\u0437\u0430</code>\n"
                "/chats \u2014 \u043c\u043e\u043d\u0438\u0442\u043e\u0440\u0438\u043c\u044b\u0435 \u0447\u0430\u0442\u044b\n"
                "/stats \u2014 \u0441\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0430\n"
                "/status \u2014 \u0441\u0442\u0430\u0442\u0443\u0441 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u0438\u044f")
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
                lines = [f"\u2022 {html.escape(n)} (<code>{cid}</code>)" for n, cid in self._chats_cache[:50]]
                await self.send(cid, "\n".join(lines))
            else:
                await self.send(cid, "Not loaded yet")
        elif text == "/stats":
            s = self.state.stats; t, r = s["total_triggers"], s["reacted"]
            p = round(r/t*100,1) if t else 0
            await self.send(cid, f"Triggers: {t} | Reacted: {r} ({p}%) | Pending: {t-r}")
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
            logger.exception("Command handler error")
            cid = msg.get("chat", {}).get("id")
            if cid:
                await self.send(cid, f"\u274c Error: {e}")

    async def _handle_cb(self, cb: dict):
        cid = cb["message"]["chat"]["id"]
        mid = cb["message"]["message_id"]
        un = cb.get("from", {}).get("username") or cb.get("from", {}).get("first_name", "?")
        data = cb.get("data", "")
        if data == "react":
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
        logger.info("Bot polling started")
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
                        elif self._auth_callback and txt:
                            # During auth flow, any non-command text = auth code
                            await self._auth_callback(txt)
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
        self.custom_pats = build_custom_patterns(self.state.keywords)
        self.cache: TTLCache = TTLCache(maxsize=10_000, ttl=DEDUP_TTL)
        self.bot = BotAPI(BOT_TOKEN, self.state)
        self.client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
        self.watched: set[int] = set()
        self._me_id: Optional[int] = None
        self._waiting_auth = False
        self._auth_event = asyncio.Event()
        self._auth_code: Optional[str] = None

    async def _check_session(self) -> bool:
        """Check if existing session is valid."""
        sess = Path(f"{SESSION_NAME}.session").exists()
        if not sess:
            return False
        try:
            await self.client.connect()
            if await self.client.is_user_authorized():
                logger.info("Session valid, authorized"); return True
            logger.warning("Session not authorized, removing")
            await self.client.disconnect()
            p = Path(f"{SESSION_NAME}.session")
            if p.exists(): p.unlink()
            # Recreate client so it's clean for future auth
            self.client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
        except Exception as e:
            logger.warning("Session invalid (%s), removing", e)
            try: await self.client.disconnect()
            except Exception: pass
            p = Path(f"{SESSION_NAME}.session")
            if p.exists(): p.unlink()
            # Recreate client
            self.client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
        return False

    async def _do_auth(self, chat_id: int):
        """Run auth flow via bot using client.start()."""
        if not PHONE:
            await self.bot.send(chat_id, "\u274c PHONE env var not set"); return
        # Check/clean session once
        if await self._check_session():
            await self.bot.send(chat_id, "\u2705 Already authorized!")
            await self._post_auth(); return

        logger.info("Starting auth flow for %s via client.start()", PHONE)
        await self.bot.send(chat_id,
            f"\U0001f510 <b>Auth</b>\nRequesting code for <code>{PHONE}</code>...")

        # Use asyncio Events to bridge bot polling -> Telethon callbacks
        code_event = asyncio.Event()
        code_value = [None]  # mutable container

        async def get_code():
            """Called by Telethon when it needs the login code."""
            await self.bot.send(chat_id,
                f"\U0001f4e9 Code sent to <code>{PHONE}</code>\n\n"
                f"Enter code here (digits only):")
            code_event.clear()
            code_value[0] = None

            async def _on_code(c):
                code_value[0] = c
                code_event.set()

            self.bot._auth_callback = _on_code
            try:
                await asyncio.wait_for(code_event.wait(), timeout=300)
            except asyncio.TimeoutError:
                pass
            finally:
                self.bot._auth_callback = None
            return code_value[0] or ""

        pwd_event = asyncio.Event()
        pwd_value = [None]

        async def get_password():
            """Called by Telethon when it needs 2FA password."""
            await self.bot.send(chat_id, "\u26a0\ufe0f Enter 2FA password:")
            pwd_event.clear()
            pwd_value[0] = None

            async def _on_pwd(p):
                pwd_value[0] = p
                pwd_event.set()

            self.bot._auth_callback = _on_pwd
            try:
                await asyncio.wait_for(pwd_event.wait(), timeout=300)
            except asyncio.TimeoutError:
                pass
            finally:
                self.bot._auth_callback = None
            return pwd_value[0] or ""

        try:
            # client.start() handles connect + send_code + sign_in automatically
            await self.client.start(
                phone=PHONE,
                code_callback=get_code,
                password=get_password,
                force_sms=True
            )
            logger.info("Authorized successfully via client.start()!")
            await self.bot.send(chat_id, "\u2705 <b>Authorized!</b> Starting monitor...")
            await self._post_auth()
        except Exception as e:
            logger.exception("Auth via start() failed")
            await self.bot.send(chat_id, f"\u274c Auth error: {e}")

    async def _post_auth(self):
        """Called after successful authorization."""
        await self.discover_chats()
        self.client.add_event_handler(self.on_msg, events.NewMessage())
        logger.info("Listening %d chats, target=%s", len(self.watched), self.state.target)

    async def discover_chats(self):
        me = await self.client.get_me(); self._me_id = me.id; count = 0; names = []
        logger.info("Logged in as: %s (ID: %d)", me.first_name, me.id)
        async for d in self.client.iter_dialogs():
            if d.is_user: continue
            e = d.entity
            if getattr(e,'id',None)==me.id: continue
            if isinstance(e,(Channel,TGChat)):
                self.watched.add(e.id)
                t=getattr(e,'title','?'); u=getattr(e,'username',None)
                name = f"{t} (@{u})" if u else t
                names.append((name, e.id)); count+=1
        self.bot._chats_cache=names
        if not self.watched:
            logger.error("No chats!"); await self.client.disconnect(); sys.exit(1)
        logger.info("Discovered %d chats",count)

    def is_dup(self,c,t):
        k=dedup_key(c,t)
        if k in self.cache: return True
        self.cache[k]=True; return False

    def find_trig(self, nt: str) -> Optional[str]:
        return find_trigger(nt, self.pat, self.custom_pats)

    async def on_msg(self, ev):
        try:
            if isinstance(ev.message, MessageService):
                return
            sender = await ev.get_sender()
            if isinstance(sender, User) and sender.bot:
                logger.debug("SKIP bot: chat=%s sender=%s", ev.chat_id, sender.id)
                return
            raw = ev.raw_text or ""
            if not raw.strip():
                return
            # Log ALL incoming text messages for debugging
            is_self = isinstance(sender, User) and sender.id == self._me_id
            logger.info("MSG|chat=%s|sender=%s|out=%s|self=%s|text='%s'",
                        ev.chat_id, getattr(sender,'id','?'), ev.out, is_self, raw[:80])
            # Skip only own messages from the SAME account (not twinks)
            if is_self:
                return
            # Auto-add chat to watched if not present
            if ev.chat_id not in self.watched:
                logger.info("AUTO-ADD chat %s to watched", ev.chat_id)
                self.watched.add(ev.chat_id)
                chat = await ev.get_chat()
                ct = getattr(chat, 'title', None) or getattr(chat, 'first_name', '?')
                cu = getattr(chat, 'username', None)
                name = f"{ct} (@{cu})" if cu else ct
                self.bot._chats_cache.append((name, ev.chat_id))
            self.pat = build_pattern(self.state.keywords)
            self.custom_pats = build_custom_patterns(self.state.keywords)
            trig = self.find_trig(normalize(raw))
            if trig is None:
                logger.debug("NO TRIGGER in: '%s'", raw[:60])
                return
            if self.is_dup(ev.chat_id, raw):
                logger.debug("DUP: chat=%s", ev.chat_id)
                return
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
        # Wire up bot callbacks
        self.bot._auth_trigger = lambda cid: self._do_auth(cid)
        async def _status():
            auth = await self._check_session() if not self.watched else True
            check = "\u2705" if auth and self.watched else "\u274c"
            return (f"\U0001f4ca <b>Status</b>\n\n"
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
            logger.info("No valid session. Waiting for /auth command via bot...")
            target = self.state.target
            if target:
                await self.bot.send(target,
                    "\u26a0\ufe0f <b>Telethon not authorized!</b>\n\n"
                    "Use /auth to start authorization.")
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


if __name__=="__main__":
    try: asyncio.run(LeadMonitor().run())
    except KeyboardInterrupt: logger.info("Stopped")
