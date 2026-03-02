# Telegram-бот для приёма и обработки отчётов водителя
from __future__ import annotations

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

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# Хранение последних сообщений для контекста (на каждый чат)
MESSAGE_CONTEXT: dict[int, deque] = {}
CONTEXT_SIZE = 5

NOT_ALLOWED_MSG = "Извините, бот не может работать в этой группе."


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

    user_id = update.effective_user.id if update.effective_user else None
    message_id = update.message.message_id if update.message else None
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
