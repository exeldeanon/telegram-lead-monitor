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

# RouterAI config for lead classification
ROUTERAI_API_KEY = os.environ.get("ROUTERAI_API_KEY", "")
ROUTERAI_MODEL = os.environ.get("ROUTERAI_MODEL", "qwen/qwen-2.5-7b-instruct")
ROUTERAI_ENABLED = bool(ROUTERAI_API_KEY)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout),
              logging.FileHandler("monitor.log", encoding="utf-8")],
)
logger = logging.getLogger("lead_monitor")


# ── State Manager ───────────────────────────────────────────────────────────

class State:
    """Persistent JSON state: keywords, target, stats, reactions, dedup."""

    DEDUP_MAX_AGE = 7 * 86400  # 7 days
    DEDUP_MAX_SIZE = 50_000

    def __init__(self, path: str):
        self.path = Path(path)
        self.data = {
            "target_chat_id": DEFAULT_TARGET,
            "target_thread_id": 0,  # topic/thread ID for forum groups
            "keywords": [],
            "stats": {"total_triggers": 0, "reacted": 0, "ai_filtered": 0, "ai_passed": 0},
            "reactions": {},  # alert_msg_id -> {"user": name, "time": ts}
            "sent_hashes": {},  # dedup_hash -> timestamp
            "blacklisted_chats": [],  # chat IDs to ignore
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
            logger.info("Cleaned up %d old dedup hashes", before - after)
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
     r"\bли[дт][ыауе]?\b|\blead[sz]?\b|\bлiд\w*\b"),
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
     r"\b(проблем\w*|трудност\w*|сложност\w*|вопрос\w*)\s+с\s+\w{0,10}\s*(трафик|траф[fф]?[fф]?)\w*\b"),
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
    """Legacy text-based dedup key (kept for compatibility)."""
    return hashlib.sha256(f"{cid}:{normalize(text)}".encode()).hexdigest()[:16]


def msg_dedup_key(chat_id: int, message_id: int) -> str:
    """Unique key per message — never collides for different messages."""
    return f"{chat_id}:{message_id}"


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

    async def send_alert(self, chat_id: int, body: str, thread_id: int = 0) -> Optional[int]:
        kb = {"inline_keyboard": [[
            {"text": "\u2705 \u041e\u0442\u0440\u0435\u0430\u0433\u0438\u0440\u043e\u0432\u0430\u0442\u044c", "callback_data": "react"}
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
            logger.error("send_alert FAILED to %s (thread=%s): %s", chat_id, thread_id, resp)
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
                "/auth — авторизация Telethon\n"
                "/target <code>ID</code> — чат для алертов\n"
                "/keywords — триггеры\n"
                "/addkeyword <code>фраза</code>\n"
                "/delkeyword <code>фраза</code>\n"
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
            s = self.state.stats
            t = s.get("total_triggers", 0)
            r = s.get("reacted", 0)
            ai_f = s.get("ai_filtered", 0)
            ai_p = s.get("ai_passed", 0)
            p = round(r/t*100, 1) if t else 0
            bl = len(self.state.blacklisted_chats)
            await self.send(cid, 
                f"📊 <b>Статистика</b>\n\n"
                f"🔔 Триггеры: {t}\n"
                f"✅ Реагировали: {r} ({p}%)\n"
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


# ── RouterAI Lead Classifier ────────────────────────────────────────────────

_CLASSIFY_SYSTEM = """Ты — эксперт-классификатор для Telegram-чатов в нише лидогенерации, баз данных и арбитража трафика.

Нас интересуют ТОЛЬКО два типа сообщений:

🎯 ТИП 1 — ПРОДАВЕЦ БАЗ/ЛИДОВ (конкурент или партнёр):
Человек ПРОДАЁТ или ПРЕДЛАГАЕТ базы, лиды, данные — то же чем занимаемся мы.
Примеры: "продам базу МФО", "есть свежие лиды на кредиты", "слив базы отказников", 
"база ИП для обзвона", "лиды под банкротство", "продаю базу казино".
ВАЖНО: только если продаёт именно БАЗЫ/ЛИДЫ/ДАННЫЕ, а не товары/услуги вообще.

🎯 ТИП 2 — ЧЕЛОВЕК С ПРОБЛЕМОЙ (потенциальный клиент):
У человека ПРОБЛЕМЫ с трафиком, лидами, базами, обзвоном, конверсией.
Примеры: "проблемы с траффом", "не могу найти нормальные лиды", 
"база тухлая никто не берёт", "трафик дорогой а конверсий нет",
"где брать свежую базу", "лиды холодные все", "обзвонил 500 номеров ноль результата".

❌ НЕ ИНТЕРЕСНО:
- Продажа личных вещей: айфон, телефон, машина, квартира, одежда, еда
- Покупка обычных товаров/услуг не связанных с базами/лидами
- "База отдыха", "базовый тариф", "база по математике", "база знаний"
- "Лидер группы", "лидерство", "трафик на дороге", "интернет-трафик"
- Мемы, шутки, приветствия, оффтопик, политика, спорт
- Новостной контент без конкретного запроса или проблемы
- Рекламный спам нерелевантных услуг (дизайн, SMM, сайты)
- Просто упоминание слова без контекста продажи или проблемы

Отвечай СТРОГО в JSON (без markdown, без пояснений, без лишних символов):
{"is_lead": true/false, "confidence": 0.0-1.0, "reason": "коротко почему"}"""

_CLASSIFY_EXAMPLES = [
    {"role": "user", "content": "[Триггер: 'лиды (общее) [лиды]']\nпродам базу мфо отказники свежие 50к"},
    {"role": "assistant", "content": '{"is_lead": true, "confidence": 0.98, "reason": "тип: продавец баз — предлагает базу МФО отказников"}'},
    {"role": "user", "content": "[Триггер: 'трафик [траф]']\nпацаны у кого какие проблемы с траффом?"},
    {"role": "assistant", "content": '{"is_lead": true, "confidence": 0.95, "reason": "тип: проблема с трафиком — спрашивает о проблемах"}'},
    {"role": "user", "content": "[Триггер: 'база/базы [база]']\nпродаю айфон 15 pro max база 80к"},
    {"role": "assistant", "content": '{"is_lead": false, "confidence": 0.97, "reason": "продажа личного телефона, слово база здесь = цена"}'},
    {"role": "user", "content": "[Триггер: 'лиды (общее) [лиды]']\nлидеры группы встретились вчера"},
    {"role": "assistant", "content": '{"is_lead": false, "confidence": 0.96, "reason": "речь о лидерах музыкальной группы, не о лидах"}'},
    {"role": "user", "content": "[Триггер: 'база/базы [базу]']\nнужна база отдыха на выходные посоветуйте"},
    {"role": "assistant", "content": '{"is_lead": false, "confidence": 0.98, "reason": "база отдыха = туризм, не связано с лидогенерацией"}'},
    {"role": "user", "content": "[Триггер: 'проблемы с обзвоном [обзвонил]']\nобзвонил сегодня 300 номеров ноль результата база мертвая"},
    {"role": "assistant", "content": '{"is_lead": true, "confidence": 0.97, "reason": "тип: проблема с обзвоном — мёртвая база, нет результата"}'},
]


async def classify_lead(text: str, trigger: str) -> dict:
    """Classify if a message is a real lead or false positive using RouterAI."""
    if not ROUTERAI_ENABLED:
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
                    "Authorization": f"Bearer {ROUTERAI_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": ROUTERAI_MODEL,
                    "messages": messages,
                    "temperature": 0.1,
                    "max_tokens": 150,
                },
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                data = await resp.json()

        logger.debug("RouterAI response: %s", str(data)[:500])
        
        if "choices" not in data:
            logger.error("RouterAI: no 'choices' in response: %s", str(data)[:300])
            return {"is_lead": True, "confidence": 0.5, "reason": f"API error: {data.get('error', {}).get('message', 'unknown')}"}
            
        content = data["choices"][0]["message"]["content"].strip()
        # Extract JSON from response
        json_match = re.search(r'\{[^}]+\}', content)
        if json_match:
            result = json.loads(json_match.group())
            return {
                "is_lead": bool(result.get("is_lead", True)),
                "confidence": float(result.get("confidence", 0.5)),
                "reason": result.get("reason", ""),
            }
        logger.warning("RouterAI: could not parse JSON from response: %s", content[:100])
        return {"is_lead": True, "confidence": 0.5, "reason": "Parse error, defaulting to lead"}

    except asyncio.TimeoutError:
        logger.warning("RouterAI: timeout")
        return {"is_lead": True, "confidence": 0.5, "reason": "Timeout, defaulting to lead"}
    except Exception as e:
        logger.error("RouterAI error: %s", e)
        return {"is_lead": True, "confidence": 0.5, "reason": f"Error: {e}"}


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
        # Rate limiter: max 20 alerts per 60 seconds
        self._alert_times: list[float] = []
        self._alert_rate_limit = 20
        self._alert_rate_window = 60
        # Sender cache to avoid repeated get_sender() calls
        self._sender_cache: TTLCache = TTLCache(maxsize=5_000, ttl=3600)

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
        self.state.cleanup_old_hashes()
        await self.discover_chats()
        self.client.add_event_handler(self.on_msg, events.NewMessage())
        logger.info("Listening %d chats, target=%s, blacklisted=%d", 
                    len(self.watched), self.state.target, len(self.state.blacklisted_chats))

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

    async def _check_rate_limit(self) -> bool:
        """Returns True if rate limited (should skip alert)."""
        now = time.time()
        # Remove old entries outside window
        self._alert_times = [t for t in self._alert_times if now - t < self._alert_rate_window]
        if len(self._alert_times) >= self._alert_rate_limit:
            logger.warning("RATE LIMITED: %d alerts in %ds, skipping", 
                          len(self._alert_times), self._alert_rate_window)
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

    async def on_msg(self, ev):
        try:
            if isinstance(ev.message, MessageService):
                return

            # ── Фильтр личек: пропускаем только группы и каналы ──────────────
            chat = await ev.get_chat()
            if isinstance(chat, User):
                logger.debug("SKIP DM: chat=%s", ev.chat_id)
                return

            # ── Чёрный список чатов ───────────────────────────────────────────
            if self.state.is_blacklisted(ev.chat_id):
                logger.debug("SKIP blacklisted chat: %s", ev.chat_id)
                return

            sender = await ev.get_sender()
            if isinstance(sender, User) and sender.bot:
                logger.debug("SKIP bot: chat=%s sender=%s", ev.chat_id, sender.id)
                return
            raw = ev.raw_text or ""
            if not raw.strip():
                return

            is_self = isinstance(sender, User) and sender.id == self._me_id
            logger.info("MSG|chat=%s|sender=%s|out=%s|self=%s|text='%s'",
                        ev.chat_id, getattr(sender,'id','?'), ev.out, is_self, raw[:80])
            if is_self:
                return

            # Auto-add chat to watched if not present
            if ev.chat_id not in self.watched:
                logger.info("AUTO-ADD chat %s to watched", ev.chat_id)
                self.watched.add(ev.chat_id)
                ct0 = getattr(chat, 'title', None) or getattr(chat, 'first_name', '?')
                cu0 = getattr(chat, 'username', None)
                self.bot._chats_cache.append((f"{ct0} (@{cu0})" if cu0 else ct0, ev.chat_id))

            trig = self.find_trig(normalize(raw))
            if trig is None:
                logger.debug("NO TRIGGER in: '%s'", raw[:60])
                return
            if self.is_dup(ev.chat_id, ev.id, raw):
                logger.debug("DUP: chat=%s msg=%s", ev.chat_id, ev.id)
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

            logger.info("LEAD|chat=%s (id=%s)|trigger='%s'|sender=%s", ct, ev.chat_id, trig, sn)

            # AI classification (if enabled)
            ai_info = ""
            if ROUTERAI_ENABLED:
                try:
                    classification = await classify_lead(raw, trig)
                    logger.info("AI_CLASSIFY|is_lead=%s|confidence=%.2f|reason=%s",
                               classification["is_lead"], classification["confidence"], classification["reason"])
                    # Filter if confident it's NOT a lead (threshold 0.7)
                    if not classification["is_lead"] and classification["confidence"] >= 0.7:
                        logger.info("FALSE_POSITIVE filtered by AI (conf=%.2f): '%s'", 
                                   classification["confidence"], raw[:60])
                        self.state.inc_ai_filtered()
                        return
                    self.state.inc_ai_passed()
                    ai_info = f"\n\n🤖 <i>AI: {'✅ Лид' if classification['is_lead'] else '⚠️ Сомнительно'} ({classification['confidence']:.0%}) — {html.escape(classification['reason'])}</i>"
                except Exception as e:
                    logger.error("AI classification failed: %s", e)
                    # Continue without AI filtering on error

            # Rate limit check before sending
            if await self._check_rate_limit():
                logger.warning("RATE LIMITED, skipping alert for: '%s'", raw[:60])
                return

            cte = html.escape(ct)
            if cu:
                cte += f" (@{html.escape(cu)})"
            up = f"@{html.escape(su)} / " if su else ""

            body = (
                f"\U0001f6a8 <b>Лид!</b>\n\n"
                f"\U0001f4cc <b>Обнаружен в:</b> <a href=\"{chat_lnk}\">{cte}</a>  <code>[ID: {ev.chat_id}]</code>\n"
                f"\U0001f464 <b>Отправитель:</b> {html.escape(sn)} ({up}ID:<code>{sid}</code>)\n"
                f"\U0001f511 <b>Триггер:</b> <code>{html.escape(trig)}</code>\n\n"
                f"<blockquote>{html.escape(raw)[:3900]}</blockquote>\n\n"
                f'<a href="{lnk}">➡️ Перейти к сообщению</a>'
                f'{ai_info}'
            )

            t = self.state.target
            if t:
                mid = await self.bot.send_alert(t, body, self.state.thread_id)
                if mid:
                    self.state.inc_triggers()
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
