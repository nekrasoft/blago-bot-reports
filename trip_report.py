# Интерактивный отчёт по ходкам: /h

from __future__ import annotations

import asyncio
import logging
import mimetypes
import os
import re
from decimal import Decimal
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
from pickup_submission import send_submission
from map_client import PickupReportRejected
from trip_pickup import confirmation_payload, trip_sites, manual_site, add_trip_photos, trip_summary, prepare_trip_submission, _build_trip_row, _parse_volume, _is_private_contractor

STATE_HODKA_SELECT = 0
STATE_HODKA_COUNT = 1
STATE_HODKA_VOLUME = 2
STATE_HODKA_CASH = 3
STATE_HODKA_FILE = 4
STATE_HODKA_SITE = 5
STATE_HODKA_PHOTOS = 6
STATE_HODKA_CONFIRM = 7
logger = logging.getLogger(__name__)
HODKA_CANCEL = "hcancel"
HODKA_SKIP_FILE = "hfile_skip"
WAYBILL_MAX_FILE_SIZE_BYTES_DEFAULT = 10 * 1024 * 1024
WAYBILL_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".tif", ".tiff"}


def _clear_hodka_data(context: ContextTypes.DEFAULT_TYPE) -> None:
    for key in list(context.user_data):
        if key.startswith("hodka_"):
            context.user_data.pop(key, None)


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
                InlineKeyboardButton("30 м3", callback_data="hvol:30"),
                InlineKeyboardButton("36 м3", callback_data="hvol:36"),
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


def _hodka_action_keyboard(action: str, label: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data=action)],
                                 [InlineKeyboardButton("Отмена", callback_data=HODKA_CANCEL)]])


async def _ask_hodka_photos(update: Update, context: ContextTypes.DEFAULT_TYPE, *, waybill_token=None) -> int:
    if not context.user_data.get('hodka_site') or not context.user_data.get('hodka_counterparty_id'):
        _clear_hodka_data(context)
        await _send_hodka_text(update, 'Диалог был начат до обновления. Запустите /h заново и выберите площадку.')
        return ConversationHandler.END
    await _send_hodka_text(update, "Прикрепите до 10 фото кузова до/после вывоза одним или несколькими сообщениями. Разделять фото по типам не нужно.")
    message = update.callback_query.message if update.callback_query else update.message
    await message.reply_text("Затем нажмите «Готово / Без фото».", reply_markup=_hodka_action_keyboard("hphotos_done", "Готово / Без фото"))
    return STATE_HODKA_PHOTOS


async def hodka_save_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if len(context.user_data.get('hodka_photos') or []) >= 10:
        await update.message.reply_text('Уже загружено 10 фото. Нажмите «Готово».')
        return STATE_HODKA_PHOTOS
    try:
        file = await _download_telegram_waybill(update.message)
        count = add_trip_photos(context.user_data, [file])
        await update.message.reply_text(f"Фото: {count}/10", reply_markup=_hodka_action_keyboard("hphotos_done", "Готово"))
    except ValueError as e:
        await update.message.reply_text(str(e))
    except Exception:
        logger.exception("Не удалось загрузить фото ходки")
        await update.message.reply_text("Фото не загружено. Попробуйте ещё раз.")
    return STATE_HODKA_PHOTOS


async def hodka_photos_done(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    if query.data == HODKA_CANCEL:
        return await _cancel_hodka_callback(update, context)
    await query.edit_message_text(trip_summary(context.user_data), reply_markup=_hodka_action_keyboard(confirmation_payload(context.user_data), "Подтвердить отчёт"))
    return STATE_HODKA_CONFIRM


async def _cancel_hodka_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    _clear_hodka_data(context)
    await update.callback_query.edit_message_text("Отменено.")
    return ConversationHandler.END


async def hodka_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    if query.data == HODKA_CANCEL:
        return await _cancel_hodka_callback(update, context)
    if query.data != confirmation_payload(context.user_data):
        await _send_hodka_text(update, 'Эта сводка устарела. Подтвердите текущий отчёт.')
        return STATE_HODKA_CONFIRM
    try:
        user = update.effective_user
        submission = prepare_trip_submission(context.user_data, source="telegram", user_id=user.id,
            name=user.full_name, chat_id=update.effective_chat.id)
        context.application.mark_data_for_update_persistence(user_ids=user.id)
        await context.application.update_persistence()
        await context.application.persistence.flush()
        await asyncio.to_thread(send_submission, submission)
    except PickupReportRejected:
        logger.exception("API отклонило отчёт по ходкам")
        await query.edit_message_text("Отчёт не принят. Проверьте данные у диспетчера. Нажмите /cancel, затем запустите /h заново.")
        return STATE_HODKA_CONFIRM
    except ValueError as e:
        await query.edit_message_text(str(e))
        return STATE_HODKA_CONFIRM
    except Exception:
        logger.exception("Не удалось подтвердить сохранение ходки")
        await query.edit_message_text("Не удалось получить подтверждение. Данные сохранены; повторная отправка не создаст дубликат.",
            reply_markup=_hodka_action_keyboard(confirmation_payload(context.user_data), "Повторить подтверждение"))
        return STATE_HODKA_CONFIRM
    await query.edit_message_text("Отчёт принят.\n\n" + trip_summary(context.user_data))
    _clear_hodka_data(context)
    return ConversationHandler.END


async def hodka_select_site(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    if query.data == HODKA_CANCEL:
        return await _cancel_hodka_callback(update, context)
    if query.data == "hsite:manual":
        await query.edit_message_text("Введите адрес или район площадки одной строкой. Например: Производственная, 21")
        return STATE_HODKA_SITE
    try:
        site = context.user_data['hodka_sites'][int(query.data.split(':')[1])]
    except (KeyError, ValueError, IndexError):
        await query.edit_message_text("Список устарел. Запустите /h заново.")
        return ConversationHandler.END
    context.user_data['hodka_site'] = site
    await query.edit_message_text("Введите количество ходок (целое число):")
    return STATE_HODKA_COUNT


async def hodka_manual_site(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        context.user_data['hodka_site'] = manual_site(update.message.text or '')
    except ValueError as e:
        await update.message.reply_text(str(e))
        return STATE_HODKA_SITE
    await update.message.reply_text("Введите количество ходок (целое число):")
    return STATE_HODKA_COUNT


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
    context.user_data["hodka_counterparty_id"] = counterparties[idx]['id']
    sites = await asyncio.to_thread(trip_sites, counterparties[idx]['id'])
    context.user_data['hodka_sites'] = sites
    buttons = [[InlineKeyboardButton(f"{site['district']}, {site['address']}"[:100], callback_data=f"hsite:{i}")] for i, site in enumerate(sites)]
    buttons += [[InlineKeyboardButton("Другая площадка / Ввести адрес", callback_data="hsite:manual")],
                [InlineKeyboardButton("Отмена", callback_data=HODKA_CANCEL)]]
    await query.edit_message_text(f"Контрагент: {title}\nВыберите площадку:", reply_markup=InlineKeyboardMarkup(buttons))
    return STATE_HODKA_SITE


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
    await update.message.reply_text(
        "Выберите кузов или введите общий объём вывезенного мусора вручную:",
        reply_markup=_build_volume_keyboard(),
    )
    return STATE_HODKA_VOLUME


async def _ask_waybill(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
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


async def _ask_cash_or_waybill(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    contractor = (context.user_data.get("hodka_selected_contractor") or "").strip()
    if not _is_private_contractor(contractor):
        return await _ask_waybill(update, context)

    await _send_hodka_text(update, "Введите сумму полученной налички:")
    return STATE_HODKA_CASH


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
    return await _ask_cash_or_waybill(update, context)


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
    return await _ask_cash_or_waybill(update, context)


async def hodka_save_cash(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message:
        return STATE_HODKA_CASH

    text = (update.message.text or "").strip()
    if text.lower() in {"отмена", "cancel", "/cancel"}:
        _clear_hodka_data(context)
        await update.message.reply_text("Отменено.")
        return ConversationHandler.END

    amount = _parse_volume(text)
    if amount is None:
        await update.message.reply_text(
            "Введите сумму полученной налички, например: 10000"
        )
        return STATE_HODKA_CASH

    context.user_data["hodka_cash_income"] = _format_volume(amount)
    await update.message.reply_text(f"Наличка: {_format_volume(amount)}")
    return await _ask_hodka_photos(update, context, waybill_token=None)


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
    return await _ask_hodka_photos(update, context, waybill_token=None)


async def hodka_save_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message:
        return STATE_HODKA_FILE

    try:
        context.user_data['hodka_waybill'] = await _download_telegram_waybill(update.message)
    except ValueError as e:
        await update.message.reply_text(str(e))
        return STATE_HODKA_FILE
    except Exception:
        logger.exception("Не удалось загрузить путевой лист ходки")
        await update.message.reply_text("Не удалось загрузить путевой лист. Попробуйте ещё раз.")
        return STATE_HODKA_FILE
    return await _ask_hodka_photos(update, context)


async def hodka_file_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message:
        return STATE_HODKA_FILE
    text = (update.message.text or "").strip().lower()
    if text in {"отмена", "cancel", "/cancel"}:
        _clear_hodka_data(context)
        await update.message.reply_text("Отменено.")
        return ConversationHandler.END
    if text in {"нет", "не", "no", "skip", "пропустить", "без файла"}:
        return await _ask_hodka_photos(update, context, waybill_token=None)
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
            STATE_HODKA_SITE: [
                CallbackQueryHandler(hodka_select_site, pattern=r"^(hsite:(\d+|manual)|hcancel)$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, hodka_manual_site),
            ],
            STATE_HODKA_PHOTOS: [
                CallbackQueryHandler(hodka_photos_done, pattern=r"^(hphotos_done|hcancel)$"),
                MessageHandler((filters.PHOTO | filters.Document.ALL) & ~filters.COMMAND, hodka_save_photo),
            ],
            STATE_HODKA_CONFIRM: [CallbackQueryHandler(hodka_confirm, pattern=r"^(hconfirm:[a-f0-9]{32}|hcancel)$")],
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
            STATE_HODKA_CASH: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, hodka_save_cash),
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
        name="hodka_report",
        persistent=True,
        per_message=False,
        per_chat=False,
        per_user=True,
    )
