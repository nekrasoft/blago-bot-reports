# Telegram-бот для приёма и обработки отчётов водителя
from __future__ import annotations

import json
import logging
import os
from collections import deque
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from telegram import ReactionTypeEmoji, Update
from telegram.ext import Application, ContextTypes, MessageHandler, ChatMemberHandler, filters

from parser import parse_message
from sheets_client import append_rows
from map_client import update_map_pickup_dates

# Загрузка переменных окружения
load_dotenv(Path(__file__).resolve().parent / ".env")

PROJECT_ROOT = Path(__file__).resolve().parent
PROCESSED_MESSAGES_FILE = PROJECT_ROOT / "data" / "processed_messages.json"
MAX_PROCESSED_CACHE = 10000

# Кэш в памяти для быстрой проверки
_PROCESSED_CACHE: set[tuple[int, int]] | None = None

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# Хранение последних сообщений для контекста (на каждый чат)
MESSAGE_CONTEXT: dict[int, deque] = {}
CONTEXT_SIZE = 5

NOT_ALLOWED_MSG = "Извините, бот не может работать в этой группе."


def _get_processed_cache() -> set[tuple[int, int]]:
    """Получение кэша обработанных сообщений (загрузка при первом обращении)."""
    global _PROCESSED_CACHE
    if _PROCESSED_CACHE is None:
        if not PROCESSED_MESSAGES_FILE.exists():
            _PROCESSED_CACHE = set()
        else:
            try:
                with open(PROCESSED_MESSAGES_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                _PROCESSED_CACHE = {tuple(x) for x in data.get("processed", [])}
            except Exception:
                _PROCESSED_CACHE = set()
    return _PROCESSED_CACHE


def _save_processed_message(chat_id: int, message_id: int) -> None:
    """Добавление сообщения в кэш и файл (после успешной реакции)."""
    cache = _get_processed_cache()
    cache.add((chat_id, message_id))
    if len(cache) > MAX_PROCESSED_CACHE:
        cache_list = list(cache)[-MAX_PROCESSED_CACHE:]
        cache.clear()
        cache.update(cache_list)
    PROCESSED_MESSAGES_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(PROCESSED_MESSAGES_FILE, "w", encoding="utf-8") as f:
        json.dump({"processed": [list(x) for x in cache]}, f, ensure_ascii=False)


def _is_message_processed(chat_id: int, message_id: int) -> bool:
    """Проверка: уже обработано (бот ставил реакцию)."""
    return (chat_id, message_id) in _get_processed_cache()


def _get_allowed_chat_ids() -> set[int]:
    """Разрешённые ID чатов из .env. Пустой список — без ограничений."""
    raw = os.environ.get("ALLOWED_CHAT_IDS", "").strip()
    if not raw:
        return set()
    return {int(x.strip()) for x in raw.split(",") if x.strip()}


def _is_chat_allowed(chat_id: int, chat_type: str | None) -> bool:
    """Проверка: можно ли боту работать в этом чате."""
    allowed = _get_allowed_chat_ids()
    if not allowed:
        return True
    # Только для групп и супергрупп применяем whitelist; личные чаты разрешены
    if chat_type in ("group", "supergroup"):
        return chat_id in allowed
    return True


def get_context(chat_id: int) -> deque:
    """Получение очереди контекстных сообщений для чата."""
    if chat_id not in MESSAGE_CONTEXT:
        MESSAGE_CONTEXT[chat_id] = deque(maxlen=CONTEXT_SIZE)
    return MESSAGE_CONTEXT[chat_id]


def add_to_context(chat_id: int, text: str) -> None:
    """Добавление сообщения в контекст."""
    if text and text.strip():
        get_context(chat_id).append(text.strip())


async def _reject_and_leave(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправка сообщения об отказе и выход из чата."""
    try:
        await context.bot.send_message(chat_id=chat_id, text=NOT_ALLOWED_MSG)
    except Exception:
        pass
    try:
        await context.bot.leave_chat(chat_id=chat_id)
    except Exception as e:
        logger.warning("Не удалось выйти из чата %s: %s", chat_id, e)


async def handle_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка добавления бота в группу."""
    if not update.my_chat_member:
        return
    cm = update.my_chat_member
    new_status = cm.new_chat_member.status
    if new_status not in ("member", "administrator"):
        return
    chat_id = cm.chat.id
    chat_type = cm.chat.type
    if not _is_chat_allowed(chat_id, chat_type):
        await _reject_and_leave(chat_id, context)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка входящего сообщения: парсинг и запись в таблицу."""
    if not update.message or not update.message.text:
        return

    chat_id = update.effective_chat.id if update.effective_chat else 0
    chat_type = update.effective_chat.type if update.effective_chat else None

    if not _is_chat_allowed(chat_id, chat_type):
        await _reject_and_leave(chat_id, context)
        return

    message_id = update.message.message_id if update.message else None
    if _is_message_processed(chat_id, message_id):
        logger.debug("Сообщение уже обработано, пропуск: chat_id=%s, message_id=%s", chat_id, message_id)
        return

    user_id = update.effective_user.id if update.effective_user else None
    logger.info("Новое сообщение: chat_id=%s, user_id=%s, message_id=%s", chat_id, user_id, message_id)

    text = update.message.text.strip()
    msg_date = update.message.date or datetime.now()

    # Добавляем в контекст для следующего сообщения (до парсинга текущего)
    add_to_context(chat_id, text)

    # Контекст — предыдущие сообщения (без текущего)
    context_messages = list(get_context(chat_id))[:-1]

    try:
        rows = parse_message(text, msg_date, context_messages)
    except Exception as e:
        logger.exception("Ошибка парсинга сообщения")
        await update.message.reply_text(f"Ошибка разбора: {e}")
        return

    if not rows:
        # Не отвечаем на пустые/служебные сообщения
        return

    try:
        append_rows(rows)
        map_updated = update_map_pickup_dates(rows)
        if map_updated:
            logger.info("Обновлено бункеров на карте: %s", map_updated)
        await context.bot.set_message_reaction(
            chat_id=update.effective_chat.id,
            message_id=update.message.message_id,
            reaction=[ReactionTypeEmoji("✍️")],
        )
        _save_processed_message(update.effective_chat.id, update.message.message_id)
    except Exception as e:
        logger.exception("Ошибка записи в Google Sheets")
        await update.message.reply_text(f"Ошибка записи в таблицу: {e}")


def main() -> None:
    """Запуск бота."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        raise ValueError("Задайте TELEGRAM_BOT_TOKEN в .env")

    application = Application.builder().token(token).build()
    application.add_handler(
        ChatMemberHandler(handle_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER)
    )
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)
    )

    logger.info("Бот запущен")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
