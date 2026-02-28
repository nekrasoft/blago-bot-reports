# Telegram-бот для приёма и обработки отчётов водителя

import logging
import os
from collections import deque
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, ContextTypes, MessageHandler, filters

from parser import parse_message
from sheets_client import append_rows

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


def get_context(chat_id: int) -> deque:
    """Получение очереди контекстных сообщений для чата."""
    if chat_id not in MESSAGE_CONTEXT:
        MESSAGE_CONTEXT[chat_id] = deque(maxlen=CONTEXT_SIZE)
    return MESSAGE_CONTEXT[chat_id]


def add_to_context(chat_id: int, text: str) -> None:
    """Добавление сообщения в контекст."""
    if text and text.strip():
        get_context(chat_id).append(text.strip())


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка входящего сообщения: парсинг и запись в таблицу."""
    if not update.message or not update.message.text:
        return

    text = update.message.text.strip()
    chat_id = update.effective_chat.id if update.effective_chat else 0
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
        count = append_rows(rows)
        await update.message.reply_text(f"Записано строк: {count}")
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
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)
    )

    logger.info("Бот запущен")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
