# Telegram-бот для приёма и обработки отчётов водителя
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, ContextTypes, ChatMemberHandler

from bunker_report import get_bunker_conversation_handler
from trip_report import get_hodka_conversation_handler

# Загрузка переменных окружения
load_dotenv(Path(__file__).resolve().parent / ".env")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

NOT_ALLOWED_MSG = "Извините, бот не может работать в этой группе."


def _get_allowed_chat_ids() -> set[int]:
    """Разрешённые ID чатов из .env. Пустой список — без ограничений."""
    raw = os.environ.get("ALLOWED_CHAT_IDS", "").strip()
    if not raw:
        return set()
    return {int(x.strip()) for x in raw.split(",") if x.strip()}


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Логирование и обработка исключений (сетевые ошибки, таймауты и т.п.)."""
    logger.error("Исключение при обработке обновления:", exc_info=context.error)
    if update and isinstance(update, Update) and update.effective_chat:
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="Произошла ошибка. Попробуйте позже.",
            )
        except Exception:
            pass


def _is_chat_allowed(chat_id: int, chat_type: str | None) -> bool:
    """Проверка: можно ли боту работать в этом чате."""
    allowed = _get_allowed_chat_ids()
    if not allowed:
        return True
    # Только для групп и супергрупп применяем whitelist; личные чаты разрешены
    if chat_type in ("group", "supergroup"):
        return chat_id in allowed
    return True


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


def main() -> None:
    """Запуск бота."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        raise ValueError("Задайте TELEGRAM_BOT_TOKEN в .env")

    application = Application.builder().token(token).build()
    application.add_handler(
        ChatMemberHandler(handle_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER)
    )
    application.add_handler(get_hodka_conversation_handler())
    application.add_handler(get_bunker_conversation_handler())
    application.add_error_handler(error_handler)

    logger.info("Бот запущен")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
