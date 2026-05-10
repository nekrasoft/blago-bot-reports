# Интерактивный отчёт по ходкам: /h

from __future__ import annotations

import json
import mimetypes
import os
import re
from decimal import Decimal, InvalidOperation
from io import BytesIO
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

from map_client import get_trip_removal_counterparties
from sheets_client import append_rows
from waybill_files_db import save_waybill_file
from waybill_notes import format_note_with_waybill_token, generate_waybill_token

PROJECT_ROOT = Path(__file__).resolve().parent
OPERATIONS_PATH = PROJECT_ROOT / "data" / "operations.json"

STATE_HODKA_SELECT = 0
STATE_HODKA_COUNT = 1
STATE_HODKA_VOLUME = 2
STATE_HODKA_FILE = 3
HODKA_CANCEL = "hcancel"
HODKA_SKIP_FILE = "hfile_skip"
WAYBILL_MAX_FILE_SIZE_BYTES_DEFAULT = 10 * 1024 * 1024
WAYBILL_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".tif", ".tiff"}


def _clear_hodka_data(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("hodka_counterparties", None)
    context.user_data.pop("hodka_selected_contractor", None)
    context.user_data.pop("hodka_trips_count", None)
    context.user_data.pop("hodka_volume_note", None)
    context.user_data.pop("hodka_date_str", None)
    context.user_data.pop("hodka_waybill_token", None)


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


def _build_trip_row(
    contractor: str,
    trips_count: int,
    date_str: str,
    note: str = "",
) -> dict:
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
        "Примечание": note,
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


def _build_waybill_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Пропустить путевой лист", callback_data=HODKA_SKIP_FILE)],
            [InlineKeyboardButton("Отмена", callback_data=HODKA_CANCEL)],
        ]
    )


def _build_volume_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Полный кузов 30 кубов", callback_data="hvol:30"),
                InlineKeyboardButton("Полный кузов 36 кубов", callback_data="hvol:36"),
            ],
            [InlineKeyboardButton("Отмена", callback_data=HODKA_CANCEL)],
        ]
    )


def _parse_trips_count(text: str) -> int | None:
    m = re.search(r"\d+", text or "")
    if not m:
        return None
    value = int(m.group(0))
    return value if value > 0 else None


def _parse_volume(text: str) -> Decimal | None:
    match = re.search(r"\d+(?:[,.]\d+)?", text or "")
    if not match:
        return None
    try:
        value = Decimal(match.group(0).replace(",", "."))
    except InvalidOperation:
        return None
    return value if value > 0 else None


def _format_volume(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == normalized.to_integral():
        return str(normalized.quantize(Decimal("1")))
    return format(normalized, "f").rstrip("0").rstrip(".").replace(".", ",")


def _volume_note(value: Decimal) -> str:
    return f"Объем: {_format_volume(value)} м3"


def _get_waybill_max_file_size_bytes() -> int:
    raw = os.environ.get("WAYBILL_MAX_FILE_SIZE_BYTES", "").strip()
    if not raw:
        return WAYBILL_MAX_FILE_SIZE_BYTES_DEFAULT
    try:
        value = int(raw)
    except ValueError:
        return WAYBILL_MAX_FILE_SIZE_BYTES_DEFAULT
    return value if value > 0 else WAYBILL_MAX_FILE_SIZE_BYTES_DEFAULT


def _is_supported_waybill_type(file_name: str | None, content_type: str | None) -> bool:
    content_type = (content_type or "").lower()
    suffix = Path(file_name or "").suffix.lower()
    return (
        content_type == "application/pdf"
        or content_type.startswith("image/")
        or suffix == ".pdf"
        or suffix in WAYBILL_IMAGE_EXTENSIONS
    )


def _detect_waybill_content_type(
    file_name: str | None,
    content_type: str | None,
    file_bytes: bytes,
) -> str | None:
    if file_bytes.startswith(b"%PDF"):
        return "application/pdf"
    if file_bytes.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if file_bytes.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if file_bytes.startswith(b"GIF87a") or file_bytes.startswith(b"GIF89a"):
        return "image/gif"
    if file_bytes.startswith(b"RIFF") and file_bytes[8:12] == b"WEBP":
        return "image/webp"
    guessed, _ = mimetypes.guess_type(file_name or "")
    return content_type or guessed


async def _download_telegram_waybill(message) -> dict:
    max_size = _get_waybill_max_file_size_bytes()
    source_file_id = ""
    file_name = ""
    content_type = ""
    file_size = 0

    if message.photo:
        attachment = message.photo[-1]
        source_file_id = attachment.file_id
        file_unique_id = getattr(attachment, "file_unique_id", "") or source_file_id
        file_name = f"waybill_{file_unique_id}.jpg"
        content_type = "image/jpeg"
        file_size = int(getattr(attachment, "file_size", 0) or 0)
    elif message.document:
        attachment = message.document
        source_file_id = attachment.file_id
        file_name = attachment.file_name or f"waybill_{attachment.file_unique_id}"
        content_type = attachment.mime_type or mimetypes.guess_type(file_name)[0] or ""
        file_size = int(getattr(attachment, "file_size", 0) or 0)
        if not _is_supported_waybill_type(file_name, content_type):
            raise ValueError("Можно загрузить только картинку или PDF.")
    else:
        raise ValueError("Загрузите картинку или PDF с путевым листом.")

    if file_size and file_size > max_size:
        raise ValueError(f"Файл слишком большой. Лимит: {max_size // (1024 * 1024)} МБ.")

    telegram_file = await attachment.get_file()
    buffer = BytesIO()
    await telegram_file.download_to_memory(buffer)
    file_bytes = buffer.getvalue()
    if len(file_bytes) > max_size:
        raise ValueError(f"Файл слишком большой. Лимит: {max_size // (1024 * 1024)} МБ.")

    detected_content_type = _detect_waybill_content_type(file_name, content_type, file_bytes)
    if not _is_supported_waybill_type(file_name, detected_content_type):
        raise ValueError("Можно загрузить только картинку или PDF.")

    return {
        "file_bytes": file_bytes,
        "file_name": file_name,
        "content_type": detected_content_type,
        "source_file_id": source_file_id,
    }


async def _send_hodka_text(update: Update, text: str) -> None:
    if update.callback_query and update.callback_query.message:
        await update.callback_query.message.reply_text(text)
    elif update.message:
        await update.message.reply_text(text)


async def _append_hodka_report(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    waybill_token: str | None,
) -> int:
    contractor = (context.user_data.get("hodka_selected_contractor") or "").strip()
    trips_count = context.user_data.get("hodka_trips_count")
    volume_note = (context.user_data.get("hodka_volume_note") or "").strip()
    date_str = (context.user_data.get("hodka_date_str") or "").strip()
    if not contractor or not trips_count or not volume_note or not date_str:
        _clear_hodka_data(context)
        await _send_hodka_text(update, "Данные отчёта устарели. Запустите /h заново.")
        return ConversationHandler.END

    note = format_note_with_waybill_token(volume_note, waybill_token) if waybill_token else volume_note
    row = _build_trip_row(contractor, int(trips_count), date_str, note)

    try:
        append_rows([row])
    except Exception as e:
        await _send_hodka_text(update, f"Ошибка записи в таблицу: {e}")
        return STATE_HODKA_FILE

    _clear_hodka_data(context)
    suffix = " Путевой лист принят." if waybill_token else " Путевой лист пропущен."
    await _send_hodka_text(
        update,
        f"Записано: {contractor}, ходок: {trips_count}, {volume_note}.{suffix}",
    )
    return ConversationHandler.END


async def hodka_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Команда /h — ходка/рейс: выбор trip_removal-контрагента и ввод количества."""
    if not update.message:
        return ConversationHandler.END

    counterparties = get_trip_removal_counterparties()
    counterparties = [c for c in counterparties if _counterparty_title(c)]
    if not counterparties:
        await update.message.reply_text(
            "Не найдено контрагентов с operation_type=trip_removal."
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
    """Выбор контрагента из списка trip_removal."""
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
    """Приём количества ходок и переход к выбору объёма."""
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
    context.user_data["hodka_trips_count"] = trips_count
    context.user_data["hodka_date_str"] = date_str
    context.user_data["hodka_waybill_token"] = generate_waybill_token()
    await update.message.reply_text(
        "Выберите кузов или введите общий объём вывезенного мусора вручную:",
        reply_markup=_build_volume_keyboard(),
    )
    return STATE_HODKA_VOLUME


async def _ask_waybill(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if "hodka_waybill_token" not in context.user_data:
        context.user_data["hodka_waybill_token"] = generate_waybill_token()
    text = (
        "Загрузите путевой лист: фото или PDF подписанной мастером бумаги.\n"
        "Если путевого листа нет, нажмите «Пропустить путевой лист»."
    )
    if update.callback_query and update.callback_query.message:
        await update.callback_query.message.reply_text(
            text,
            reply_markup=_build_waybill_keyboard(),
        )
    elif update.message:
        await update.message.reply_text(
            text,
            reply_markup=_build_waybill_keyboard(),
        )
    return STATE_HODKA_FILE


async def hodka_volume_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if not query:
        return STATE_HODKA_VOLUME
    await query.answer()

    data = query.data or ""
    if data == HODKA_CANCEL:
        _clear_hodka_data(context)
        await query.edit_message_text("Отменено.")
        return ConversationHandler.END
    if not data.startswith("hvol:"):
        return STATE_HODKA_VOLUME

    trips_count = context.user_data.get("hodka_trips_count")
    if not trips_count:
        _clear_hodka_data(context)
        await query.edit_message_text("Данные отчёта устарели. Запустите /h заново.")
        return ConversationHandler.END

    body_volume = _parse_volume(data.replace("hvol:", "", 1))
    if body_volume is None:
        await query.answer("Некорректный объём.", show_alert=True)
        return STATE_HODKA_VOLUME

    total_volume = body_volume * int(trips_count)
    context.user_data["hodka_volume_note"] = _volume_note(total_volume)
    await query.edit_message_text(
        f"Объём: {_format_volume(total_volume)} м3 ({_format_volume(body_volume)} м3 × {trips_count} ход.)"
    )
    return await _ask_waybill(update, context)


async def hodka_save_volume(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message:
        return STATE_HODKA_VOLUME

    text = (update.message.text or "").strip()
    if text.lower() in {"отмена", "cancel", "/cancel"}:
        _clear_hodka_data(context)
        await update.message.reply_text("Отменено.")
        return ConversationHandler.END

    volume = _parse_volume(text)
    if volume is None:
        await update.message.reply_text(
            "Выберите кузов кнопкой или введите общий объём, например: 10"
        )
        return STATE_HODKA_VOLUME

    context.user_data["hodka_volume_note"] = _volume_note(volume)
    await update.message.reply_text(f"Объём: {_format_volume(volume)} м3")
    return await _ask_waybill(update, context)


async def hodka_skip_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if not query:
        return STATE_HODKA_FILE
    await query.answer()

    data = query.data or ""
    if data == HODKA_CANCEL:
        _clear_hodka_data(context)
        await query.edit_message_text("Отменено.")
        return ConversationHandler.END
    if data != HODKA_SKIP_FILE:
        return STATE_HODKA_FILE

    await query.edit_message_text("Путевой лист пропущен.")
    return await _append_hodka_report(update, context, waybill_token=None)


async def hodka_save_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message:
        return STATE_HODKA_FILE

    token = (context.user_data.get("hodka_waybill_token") or "").strip()
    if not token:
        token = generate_waybill_token()
        context.user_data["hodka_waybill_token"] = token

    try:
        file_info = await _download_telegram_waybill(update.message)
        save_waybill_file(
            file_token=token,
            source="telegram",
            file_bytes=file_info["file_bytes"],
            source_chat_id=update.effective_chat.id if update.effective_chat else None,
            source_user_id=update.effective_user.id if update.effective_user else None,
            source_message_id=update.message.message_id,
            source_file_id=file_info["source_file_id"],
            file_name=file_info["file_name"],
            content_type=file_info["content_type"],
        )
    except ValueError as e:
        await update.message.reply_text(str(e))
        return STATE_HODKA_FILE
    except Exception as e:
        await update.message.reply_text(
            "Не удалось сохранить путевой лист в БД. "
            f"Попробуйте отправить файл ещё раз или пропустите его. Ошибка: {e}"
        )
        return STATE_HODKA_FILE

    return await _append_hodka_report(update, context, waybill_token=token)


async def hodka_file_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message:
        return STATE_HODKA_FILE
    text = (update.message.text or "").strip().lower()
    if text in {"отмена", "cancel", "/cancel"}:
        _clear_hodka_data(context)
        await update.message.reply_text("Отменено.")
        return ConversationHandler.END
    if text in {"нет", "не", "no", "skip", "пропустить", "без файла"}:
        return await _append_hodka_report(update, context, waybill_token=None)
    await update.message.reply_text(
        "Отправьте фото или PDF путевого листа либо нажмите «Пропустить путевой лист».",
        reply_markup=_build_waybill_keyboard(),
    )
    return STATE_HODKA_FILE


async def hodka_unsupported_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.message:
        await update.message.reply_text("Можно загрузить только картинку или PDF.")
    return STATE_HODKA_FILE


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
            STATE_HODKA_VOLUME: [
                CallbackQueryHandler(
                    hodka_volume_callback,
                    pattern=r"^(hvol:(30|36)|hcancel)$",
                ),
                MessageHandler(filters.TEXT & ~filters.COMMAND, hodka_save_volume),
            ],
            STATE_HODKA_FILE: [
                CallbackQueryHandler(
                    hodka_skip_file,
                    pattern=r"^(hfile_skip|hcancel)$",
                ),
                MessageHandler(
                    (filters.PHOTO | filters.Document.ALL) & ~filters.COMMAND,
                    hodka_save_file,
                ),
                MessageHandler(filters.TEXT & ~filters.COMMAND, hodka_file_text),
                MessageHandler(filters.ATTACHMENT & ~filters.COMMAND, hodka_unsupported_file),
            ],
        },
        fallbacks=[CommandHandler("cancel", hodka_cancel)],
        per_message=False,
        per_chat=False,
        per_user=True,
    )
