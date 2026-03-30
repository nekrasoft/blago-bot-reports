# Интерактивный отчёт по ходкам: /h

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from map_client import get_daily_counterparties
from sheets_client import append_rows

PROJECT_ROOT = Path(__file__).resolve().parent
OPERATIONS_PATH = PROJECT_ROOT / "data" / "operations.json"

STATE_HODKA_SELECT = 0
STATE_HODKA_COUNT = 1
HODKA_CANCEL = "hcancel"


def _clear_hodka_data(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("hodka_counterparties", None)
    context.user_data.pop("hodka_selected_contractor", None)


def _load_trip_operation() -> dict:
    default = {
        "структура": "ЮЛ - Вывоз мусора",
        "ксп": "1201",
        "операция": "Поступление по основной деятельности",
        "ксз": "1001",
    }
    try:
        with open(OPERATIONS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("trip_removal", default) or default
    except Exception:
        return default


def _build_trip_row(contractor: str, trips_count: int, date_str: str) -> dict:
    """Строка для таблицы: ходка/рейс (trip_removal)."""
    try:
        dt = datetime.strptime(date_str, "%d.%m.%Y")
    except ValueError:
        dt = datetime.now()

    op = _load_trip_operation()
    return {
        "Дата": date_str,
        "Месяц": str(dt.month),
        "Структура": op.get("структура", "ЮЛ - Вывоз мусора"),
        "КСП": op.get("ксп", "1201"),
        "Операция": op.get("операция", "Поступление по основной деятельности"),
        "КСЗ": op.get("ксз", "1001"),
        "Контрагент": contractor,
        "Примечание": "",
        "Объект": str(trips_count),
    }


def _counterparty_title(item: dict) -> str:
    return (item.get("shortName") or item.get("name") or "").strip()


def _build_hodka_keyboard(counterparties: list[dict]) -> InlineKeyboardMarkup:
    buttons = []
    for idx, item in enumerate(counterparties):
        title = _counterparty_title(item)
        if title:
            buttons.append(
                [InlineKeyboardButton(title, callback_data=f"hctr:{idx}")]
            )
    buttons.append([InlineKeyboardButton("Отмена", callback_data=HODKA_CANCEL)])
    return InlineKeyboardMarkup(buttons)


def _parse_trips_count(text: str) -> int | None:
    m = re.search(r"\d+", text or "")
    if not m:
        return None
    value = int(m.group(0))
    return value if value > 0 else None


async def hodka_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Команда /h — ходка/рейс: выбор daily-контрагента и ввод количества."""
    if not update.message:
        return ConversationHandler.END

    counterparties = get_daily_counterparties()
    counterparties = [c for c in counterparties if _counterparty_title(c)]
    if not counterparties:
        await update.message.reply_text(
            "Не найдено контрагентов с расписанием daily."
        )
        return ConversationHandler.END

    _clear_hodka_data(context)
    context.user_data["hodka_counterparties"] = counterparties

    await update.message.reply_text(
        "Команда /h (ходка/рейс).\nВыберите контрагента, для которого были сделаны ходки:",
        reply_markup=_build_hodka_keyboard(counterparties),
    )
    return STATE_HODKA_SELECT


async def hodka_select_counterparty(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    """Выбор контрагента из списка daily."""
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    await query.answer()

    data = query.data or ""
    if data == HODKA_CANCEL:
        _clear_hodka_data(context)
        await query.edit_message_text("Отменено.")
        return ConversationHandler.END

    if not data.startswith("hctr:"):
        return STATE_HODKA_SELECT

    try:
        idx = int(data.replace("hctr:", "", 1))
    except ValueError:
        await query.answer("Некорректный выбор.", show_alert=True)
        return STATE_HODKA_SELECT

    counterparties = context.user_data.get("hodka_counterparties", [])
    if not isinstance(counterparties, list) or not (0 <= idx < len(counterparties)):
        _clear_hodka_data(context)
        await query.edit_message_text("Список устарел. Запустите /h заново.")
        return ConversationHandler.END

    title = _counterparty_title(counterparties[idx])
    if not title:
        await query.answer("Контрагент не найден.", show_alert=True)
        return STATE_HODKA_SELECT

    context.user_data["hodka_selected_contractor"] = title
    await query.edit_message_text(
        f"Контрагент: {title}\nВведите количество ходок (целое число):"
    )
    return STATE_HODKA_COUNT


async def hodka_save_count(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Приём количества ходок и запись строки trip_removal в Google Sheets."""
    if not update.message:
        return STATE_HODKA_COUNT

    trips_count = _parse_trips_count((update.message.text or "").strip())
    if not trips_count:
        await update.message.reply_text(
            "Введите положительное целое число, например: 2"
        )
        return STATE_HODKA_COUNT

    contractor = (context.user_data.get("hodka_selected_contractor") or "").strip()
    if not contractor:
        _clear_hodka_data(context)
        await update.message.reply_text("Контрагент не выбран. Запустите /h заново.")
        return ConversationHandler.END

    date_str = datetime.now().strftime("%d.%m.%Y")
    row = _build_trip_row(contractor, trips_count, date_str)

    try:
        append_rows([row])
    except Exception as e:
        await update.message.reply_text(f"Ошибка записи в таблицу: {e}")
        return STATE_HODKA_COUNT

    _clear_hodka_data(context)
    await update.message.reply_text(
        f"Записано: {contractor}, ходок: {trips_count}."
    )
    return ConversationHandler.END


async def hodka_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отмена диалога /h."""
    _clear_hodka_data(context)
    if update.message:
        await update.message.reply_text("Отменено.")
    return ConversationHandler.END


def get_hodka_conversation_handler() -> ConversationHandler:
    return ConversationHandler(
        entry_points=[CommandHandler("h", hodka_start)],
        states={
            STATE_HODKA_SELECT: [
                CallbackQueryHandler(
                    hodka_select_counterparty,
                    pattern=r"^(hctr:\d+|hcancel)$",
                ),
            ],
            STATE_HODKA_COUNT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, hodka_save_count),
            ],
        },
        fallbacks=[CommandHandler("cancel", hodka_cancel)],
        per_message=False,
        per_chat=False,
        per_user=True,
    )
