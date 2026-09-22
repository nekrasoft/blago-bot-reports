# Интерактивный отчёт по бункеру: /bunker, /report, /zayavka

from __future__ import annotations

from datetime import datetime
import mimetypes
import os

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from bunker_pickup import (
    build_pickup_sheet_row,
    format_billing_units,
    pickup_group_key,
)

from map_client import (
    create_pickup_report,
    dedupe_bunker_items,
    get_all_bunkers,
    get_bunker_log_entry,
    get_bunker_identity_key,
    mark_bunker_filled,
)
from sheets_client import append_rows
from waybill_files_db import save_waybill_file
from waybill_notes import generate_waybill_token

STATE_BUNKER = 0
STATE_UNITS = 1
STATE_CLEANUP = 2
STATE_CLEANUP_COMMENT = 3
STATE_PHOTOS = 4
STATE_WAYBILL = 5
STATE_WAYBILL_REASON = 6
STATE_CONFIRM = 7
PAGE_SIZE = 8
CANCEL = "cancel"


def _get_sorted_bunkers() -> list[dict]:
    """Все бункеры, отсортированные по short-name, district и number."""
    bunkers = get_all_bunkers()

    def _norm(value: object) -> str:
        return str(value or "").strip().lower()

    def _short_name(b: dict) -> str:
        return _norm(
            b.get("short-name")
            or b.get("short_name")
            or b.get("shortName")
            or b.get("contractorShortName")
            or b.get("contractor")
        )

    def _district(b: dict) -> str:
        return _norm(b.get("district") or b.get("District"))

    def _number(b: dict) -> str:
        return _norm(b.get("number"))

    return sorted(
        bunkers,
        key=lambda b: (
            _short_name(b),
            _norm(b.get("contractor")),
            _district(b),
            _number(b),
        ),
    )


def _bunker_fill_level(bunker: dict) -> float:
    """Текущая заполненность бункера из API карты."""
    raw = (
        bunker.get("fillLevel")
        if "fillLevel" in bunker
        else bunker.get("fill_level", bunker.get("filllevel", 0))
    )
    if raw is None:
        return 0.0
    if isinstance(raw, str):
        raw = raw.strip().rstrip("%").replace(",", ".")
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


def _filter_bunkers_by_mode(bunkers: list[dict], mode: str) -> list[dict]:
    """Для /z показываем <100%, для /b — только 100%."""
    if mode == "request":
        return [b for b in bunkers if not b.get("pendingRequestId")]
    return [b for b in bunkers if b.get("pendingRequestId")]


def _get_available_bunkers(
    mode: str = "report",
    exclude_ids: set | frozenset | None = None,
    exclude_keys: set | frozenset | None = None,
    group_key: tuple[str, str] | None = None,
) -> list[dict]:
    exclude = exclude_ids or set()
    key_exclude = exclude_keys or set()
    result: list[dict] = []
    seen_keys: set[str] = set()
    for bunker in _filter_bunkers_by_mode(_get_sorted_bunkers(), mode):
        if group_key is not None and pickup_group_key(bunker) != group_key:
            continue
        bunker_id = bunker.get("id")
        if not bunker_id or bunker_id in exclude:
            continue
        key = get_bunker_identity_key(bunker)
        if key in key_exclude or key in seen_keys:
            continue
        seen_keys.add(key)
        result.append(bunker)
    return result


# Типы улиц для удаления из подписи кнопки
_STREET_TYPES = frozenset(
    w.lower()
    for w in (
        "улица", "ул.", "ул", "проезд", "пр.", "пр", "проспект", "пр-т", "пр-кт",
        "переулок", "пер.", "пер", "бульвар", "б-р", "шоссе", "набережная", "наб.",
        "площадь", "пл.", "пл", "тракт", "тупик", "просек",
    )
)


def _shorten_address(addr: str) -> str:
    """
    Сокращение адреса для кнопки:
    - убрать город,
    - убрать тип (улица, проезд, проспект и т.п.),
    - для именных улиц (Романа Ердякова, Дмитрия Козулева) — только фамилия.
    """
    if not addr:
        return ""
    if "," in addr:
        addr = addr.split(",", 1)[1].strip()
    words = [w.rstrip(",") for w in addr.split()]
    cleaned = [w for w in words if w and w.lower().rstrip(".") not in _STREET_TYPES]
    if not cleaned:
        return addr
    street_words = [w for w in cleaned if not w.lstrip("-").isdigit()]
    num_part = " ".join(w for w in cleaned if w.lstrip("-").isdigit())
    if len(street_words) == 2 and street_words[0][0].isupper() and street_words[1][0].isupper():
        street_part = street_words[-1]
    else:
        street_part = " ".join(street_words)
    result = f"{street_part}, {num_part}".strip(", ") if num_part else street_part
    return result


def _bunker_label(b: dict, max_len: int = 50) -> str:
    """Краткая подпись для кнопки: №12 · Контрагент · Район/адрес."""
    num = b.get("number", "?")
    contractor = (b.get("contractor") or "").strip()
    district = (b.get("district") or b.get("District") or "").strip()
    location = district if district else _shorten_address(b.get("address") or "")
    parts = [f"№{num}", contractor, location]
    label = " · ".join(p for p in parts if p)
    return label[:max_len] + ("…" if len(label) > max_len else "")


def _address_without_city(addr: str) -> str:
    """Адрес без названия города (Киров)."""
    if not addr:
        return ""
    for prefix in ("Киров, ", "Киров,"):
        if addr.startswith(prefix):
            return addr[len(prefix) :].strip()
    return addr


def _location_for_log_item(item: dict) -> str:
    """Локация для вывода в сообщениях: district/District, иначе адрес, иначе note."""
    district = (item.get("district") or item.get("District") or "").strip()
    if district:
        return district

    address = _address_without_city(item.get("address", ""))
    if address:
        return address

    return (item.get("note") or "").strip()


def _format_bunker_report(log: list[dict]) -> str:
    """Форматирование отчёта по вывозу контейнеров для публикации в чат."""
    date_str = datetime.now().strftime("%d.%m.%Y")
    lines = ["📋 Отчёт по вывозу контейнеров", f"Дата: {date_str}", ""]

    for item in log:
        contractor = item.get("contractor", "")
        num = item.get("number", "?")
        location = _location_for_log_item(item)
        suffix = f", {location}" if location else ""
        units = item.get("billing_units")
        units_suffix = f" × {format_billing_units(units)}" if units is not None else ""
        lines.append(f"• {contractor} — №{num}{suffix}{units_suffix}")

    lines.extend(["", f"Всего: {len(log)} контейнер(ов)"])
    return "\n".join(lines)


def _format_request_report(log: list[dict]) -> str:
    """Форматирование заявки на опустошение для публикации в чат."""
    date_str = datetime.now().strftime("%d.%m.%Y")
    lines = ["✅ Заявка принята", f"Дата: {date_str}", ""]

    for item in log:
        contractor = item.get("contractor", "")
        num = item.get("number", "?")
        location = _location_for_log_item(item)
        suffix = f", {location}" if location else ""
        lines.append(f"• {contractor} — №{num}{suffix}")

    lines.extend(["", f"Всего: {len(log)} контейнер(ов)"])
    return "\n".join(lines)


def _build_bunker_keyboard(
    page: int = 0,
    exclude_ids: set | frozenset | None = None,
    exclude_keys: set | frozenset | None = None,
    mode: str = "report",
    group_key: tuple[str, str] | None = None,
) -> InlineKeyboardMarkup:
    available = _get_available_bunkers(mode, exclude_ids, exclude_keys, group_key)
    total_pages = max(1, (len(available) + PAGE_SIZE - 1) // PAGE_SIZE)
    page = min(max(page, 0), total_pages - 1)
    chunk = available[page * PAGE_SIZE : (page + 1) * PAGE_SIZE]
    buttons = [
        [InlineKeyboardButton(_bunker_label(b), callback_data=f"bunker:{b['id']}")]
        for b in chunk
        if b.get("id")
    ]
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀ Пред", callback_data=f"page:{page - 1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("След ▶", callback_data=f"page:{page + 1}"))
    if nav:
        buttons.append(nav)
    buttons.append([InlineKeyboardButton("✅ Готово", callback_data="done")])
    buttons.append([InlineKeyboardButton("Отмена", callback_data=CANCEL)])
    return InlineKeyboardMarkup(buttons)


def _units_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("1", callback_data="units:1"),
            InlineKeyboardButton("1,5", callback_data="units:1.5"),
            InlineKeyboardButton("2", callback_data="units:2"),
        ],
        [InlineKeyboardButton("Отмена", callback_data=CANCEL)],
    ])


def _cleanup_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Прибрана", callback_data="cleanup:cleaned")],
        [InlineKeyboardButton("Уборка не требовалась", callback_data="cleanup:not_required")],
        [InlineKeyboardButton("Не прибрана", callback_data="cleanup:not_cleaned")],
        [InlineKeyboardButton("Отмена", callback_data=CANCEL)],
    ])


def _photos_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Фото загружены — дальше", callback_data="photos_done")],
        [InlineKeyboardButton("Завершить без фото", callback_data="photos_skip")],
        [InlineKeyboardButton("Отмена", callback_data=CANCEL)],
    ])


def _waybill_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Завершить без талона", callback_data="waybill_skip")],
        [InlineKeyboardButton("Отмена", callback_data=CANCEL)],
    ])


def _waybill_reason_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Мастер отсутствовал", callback_data="waybill_reason:Мастер отсутствовал")],
        [InlineKeyboardButton("Отказались подписывать", callback_data="waybill_reason:Отказались подписывать")],
        [InlineKeyboardButton("Талон будет позже", callback_data="waybill_reason:Талон будет передан позже")],
        [InlineKeyboardButton("Документ утрачен", callback_data="waybill_reason:Документ утрачен")],
        [InlineKeyboardButton("Другое", callback_data="waybill_reason:Другое")],
    ])


def _confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Подтвердить вывоз", callback_data="confirm")],
        [InlineKeyboardButton("Отмена", callback_data=CANCEL)],
    ])


def _clear_report(context: ContextTypes.DEFAULT_TYPE) -> None:
    for key in tuple(context.user_data):
        if key.startswith("bunker_"):
            context.user_data.pop(key, None)


async def _cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    _clear_report(context)
    if query:
        await query.answer()
        await query.edit_message_text("Отменено.")
    elif update.message:
        await update.message.reply_text("Отменено.")
    return ConversationHandler.END


def _selection_group(context: ContextTypes.DEFAULT_TYPE) -> tuple[str, str] | None:
    value = context.user_data.get("bunker_group_key")
    return tuple(value) if isinstance(value, (tuple, list)) and len(value) == 2 else None


async def _bunker_start_impl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message:
        return ConversationHandler.END
    _clear_report(context)
    mode = context.user_data.get("bunker_mode", "report")
    available = [b for b in _filter_bunkers_by_mode(_get_sorted_bunkers(), mode) if b.get("id")]
    if not available:
        text = "Нет бункеров без активной заявки." if mode == "request" else "Нет активных заявок на вывоз."
        await update.message.reply_text(text)
        return ConversationHandler.END
    context.user_data.update(
        bunker_mode=mode,
        bunker_page=0,
        bunker_log=[],
        bunker_selected_ids=set(),
        bunker_selected_keys=set(),
        bunker_site_photos=[],
    )
    prompt = "Выберите бункер для заявки:" if mode == "request" else "Выберите вывезенный бункер:"
    await update.message.reply_text(prompt, reply_markup=_build_bunker_keyboard(mode=mode))
    return STATE_BUNKER


async def bunker_start_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["bunker_mode"] = "report"
    return await _bunker_start_impl(update, context)


async def bunker_start_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["bunker_mode"] = "request"
    return await _bunker_start_impl(update, context)


async def page_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if not query:
        return STATE_BUNKER
    await query.answer()
    try:
        page = int((query.data or "").split(":", 1)[1])
    except (IndexError, ValueError):
        page = 0
    context.user_data["bunker_page"] = page
    await query.edit_message_text(
        "Выберите бункер:",
        reply_markup=_build_bunker_keyboard(
            page,
            context.user_data.get("bunker_selected_ids", set()),
            context.user_data.get("bunker_selected_keys", set()),
            context.user_data.get("bunker_mode", "report"),
            _selection_group(context),
        ),
    )
    return STATE_BUNKER


async def bunker_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if not query:
        return STATE_BUNKER
    data = query.data or ""
    if data == CANCEL:
        return await _cancel(update, context)
    if data == "done":
        log = context.user_data.get("bunker_log", [])
        mode = context.user_data.get("bunker_mode", "report")
        if not log:
            await query.answer("Сначала выберите бункер.", show_alert=True)
            return STATE_BUNKER
        if mode == "request":
            await query.edit_message_text(_format_request_report(log))
            _clear_report(context)
            return ConversationHandler.END
        await query.answer()
        await query.edit_message_text("Была ли прибрана территория после вывоза?", reply_markup=_cleanup_keyboard())
        return STATE_CLEANUP
    if not data.startswith("bunker:"):
        return STATE_BUNKER

    bunker_id = data.split(":", 1)[1]
    log_entry = get_bunker_log_entry(bunker_id)
    if not log_entry:
        await query.answer("Бункер не найден.", show_alert=True)
        return STATE_BUNKER
    selected_ids = context.user_data.get("bunker_selected_ids", set())
    if bunker_id in selected_ids:
        await query.answer("Этот бункер уже выбран.")
        return STATE_BUNKER

    mode = context.user_data.get("bunker_mode", "report")
    if mode == "request":
        if not mark_bunker_filled(bunker_id):
            await query.answer("Не удалось создать заявку.", show_alert=True)
            return STATE_BUNKER
        selected_ids.add(bunker_id)
        context.user_data["bunker_selected_ids"] = selected_ids
        context.user_data["bunker_log"].append(log_entry)
        await query.answer("Заявка создана ✓")
        await query.edit_message_text(
            "Выберите ещё бункер или нажмите «Готово»:",
            reply_markup=_build_bunker_keyboard(
                context.user_data.get("bunker_page", 0), selected_ids, set(), mode
            ),
        )
        return STATE_BUNKER

    group = _selection_group(context)
    if group is not None and pickup_group_key(log_entry) != group:
        await query.answer("Выберите бункер того же контрагента и площадки.", show_alert=True)
        return STATE_BUNKER
    context.user_data["bunker_pending_item"] = log_entry
    await query.answer()
    await query.edit_message_text(
        f"Укажите количество к оплате для {_bunker_label(log_entry)}:",
        reply_markup=_units_keyboard(),
    )
    return STATE_UNITS


async def units_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if not query:
        return STATE_UNITS
    if query.data == CANCEL:
        return await _cancel(update, context)
    try:
        units = float((query.data or "").split(":", 1)[1])
    except (IndexError, ValueError):
        return STATE_UNITS
    if units not in (1.0, 1.5, 2.0):
        await query.answer("Некорректное количество.", show_alert=True)
        return STATE_UNITS

    item = context.user_data.pop("bunker_pending_item", None)
    if not item:
        await query.edit_message_text("Данные устарели. Запустите /b заново.")
        return ConversationHandler.END
    item["billing_units"] = units
    context.user_data["bunker_log"].append(item)
    context.user_data["bunker_selected_ids"].add(item["id"])
    context.user_data["bunker_selected_keys"].add(get_bunker_identity_key(item))
    context.user_data["bunker_group_key"] = pickup_group_key(item)
    preview = "\n".join(
        f"• {_bunker_label(x)} × {format_billing_units(x['billing_units'])}"
        for x in context.user_data["bunker_log"]
    )
    await query.answer()
    await query.edit_message_text(
        "Добавлено:\n" + preview + "\n\nВыберите ещё бункер или нажмите «Готово»: ",
        reply_markup=_build_bunker_keyboard(
            context.user_data.get("bunker_page", 0),
            context.user_data["bunker_selected_ids"],
            context.user_data["bunker_selected_keys"],
            "report",
            _selection_group(context),
        ),
    )
    return STATE_BUNKER


async def cleanup_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if not query:
        return STATE_CLEANUP
    if query.data == CANCEL:
        return await _cancel(update, context)
    status = (query.data or "").split(":", 1)[-1]
    if status not in {"cleaned", "not_required", "not_cleaned"}:
        return STATE_CLEANUP
    context.user_data["bunker_cleanup_status"] = status
    await query.answer()
    if status == "not_cleaned":
        await query.edit_message_text("Укажите причину, почему территория не прибрана:")
        return STATE_CLEANUP_COMMENT
    await query.edit_message_text(
        "Приложите до пяти фото площадки одним альбомом или несколькими сообщениями.",
        reply_markup=_photos_keyboard(),
    )
    return STATE_PHOTOS


async def cleanup_comment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message:
        return STATE_CLEANUP_COMMENT
    comment = (update.message.text or "").strip()
    if not comment:
        await update.message.reply_text("Комментарий обязателен.")
        return STATE_CLEANUP_COMMENT
    context.user_data["bunker_cleanup_comment"] = comment[:500]
    await update.message.reply_text(
        "Приложите до пяти фото площадки одним альбомом или несколькими сообщениями.",
        reply_markup=_photos_keyboard(),
    )
    return STATE_PHOTOS


async def _download_telegram_attachment(message, *, allow_pdf: bool) -> dict:
    attachment = None
    file_name = None
    content_type = None
    if message.photo:
        attachment = message.photo[-1]
        file_name = f"photo_{attachment.file_unique_id}.jpg"
        content_type = "image/jpeg"
    elif message.document:
        attachment = message.document
        file_name = attachment.file_name or f"file_{attachment.file_unique_id}"
        content_type = attachment.mime_type or mimetypes.guess_type(file_name)[0] or ""
    allowed = {"image/jpeg", "image/png", "image/webp"}
    if allow_pdf:
        allowed.add("application/pdf")
    if attachment is None or content_type not in allowed:
        raise ValueError("Прикрепите изображение" + (" или PDF" if allow_pdf else ""))
    max_size = int(os.environ.get("PICKUP_FILE_MAX_SIZE_BYTES", 10 * 1024 * 1024))
    if int(getattr(attachment, "file_size", 0) or 0) > max_size:
        raise ValueError(f"Файл больше {max_size // (1024 * 1024)} МБ")
    telegram_file = await attachment.get_file()
    file_bytes = bytes(await telegram_file.download_as_bytearray())
    if len(file_bytes) > max_size:
        raise ValueError(f"Файл больше {max_size // (1024 * 1024)} МБ")
    return {
        "file_bytes": file_bytes,
        "file_name": file_name,
        "content_type": content_type,
        "source_file_id": attachment.file_id,
        "source_message_id": message.message_id,
    }


async def photo_received(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message:
        return STATE_PHOTOS
    photos = context.user_data.setdefault("bunker_site_photos", [])
    if len(photos) >= 5:
        await update.message.reply_text("Уже загружено пять фотографий.", reply_markup=_photos_keyboard())
        return STATE_PHOTOS
    try:
        photos.append(await _download_telegram_attachment(update.message, allow_pdf=False))
    except ValueError as error:
        await update.message.reply_text(str(error), reply_markup=_photos_keyboard())
        return STATE_PHOTOS
    await update.message.reply_text(f"Фото принято ({len(photos)}/5).", reply_markup=_photos_keyboard())
    return STATE_PHOTOS


async def photos_finished(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if not query:
        return STATE_PHOTOS
    if query.data == CANCEL:
        return await _cancel(update, context)
    if query.data == "photos_done" and not context.user_data.get("bunker_site_photos"):
        await query.answer("Сначала приложите фото или выберите «Без фото».", show_alert=True)
        return STATE_PHOTOS
    await query.answer()
    requires_waybill = bool(context.user_data["bunker_log"][0].get("requiresContainerWaybill"))
    context.user_data["bunker_waybill_required"] = requires_waybill
    prompt = "Приложите подписанный талон (фото или PDF)."
    if not requires_waybill:
        prompt = "При необходимости приложите подписанный талон (фото или PDF)."
    await query.edit_message_text(prompt, reply_markup=_waybill_keyboard())
    return STATE_WAYBILL


async def waybill_received(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message:
        return STATE_WAYBILL
    try:
        context.user_data["bunker_waybill"] = await _download_telegram_attachment(update.message, allow_pdf=True)
    except ValueError as error:
        await update.message.reply_text(str(error), reply_markup=_waybill_keyboard())
        return STATE_WAYBILL
    context.user_data.pop("bunker_waybill_missing_reason", None)
    await _show_confirmation(update, context)
    return STATE_CONFIRM


async def waybill_skipped(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if not query:
        return STATE_WAYBILL
    if query.data == CANCEL:
        return await _cancel(update, context)
    await query.answer()
    if context.user_data.get("bunker_waybill_required"):
        await query.edit_message_text("Укажите причину отсутствия талона:", reply_markup=_waybill_reason_keyboard())
        return STATE_WAYBILL_REASON
    await _show_confirmation(update, context)
    return STATE_CONFIRM


async def waybill_reason_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if not query:
        return STATE_WAYBILL_REASON
    context.user_data["bunker_waybill_missing_reason"] = (query.data or "").split(":", 1)[-1][:500]
    await query.answer()
    await _show_confirmation(update, context)
    return STATE_CONFIRM


async def _show_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    log = context.user_data.get("bunker_log", [])
    lines = [f"• №{item.get('number', '?')} × {format_billing_units(item['billing_units'])}" for item in log]
    text = (
        f"Контрагент: {log[0].get('contractor', '')}\n"
        + "\n".join(lines)
        + f"\nФото: {len(context.user_data.get('bunker_site_photos', []))}"
        + f"\nТалон: {'приложен' if context.user_data.get('bunker_waybill') else 'не приложен'}"
    )
    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=_confirm_keyboard())
    elif update.message:
        await update.message.reply_text(text, reply_markup=_confirm_keyboard())


async def confirm_pickup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if not query:
        return STATE_CONFIRM
    if query.data == CANCEL:
        return await _cancel(update, context)
    await query.answer()
    log = context.user_data.get("bunker_log", [])
    waybill = context.user_data.get("bunker_waybill")
    waybill_token = generate_waybill_token() if waybill else None
    user = update.effective_user
    try:
        if waybill and waybill_token:
            save_waybill_file(
                file_token=waybill_token,
                source="telegram",
                file_bytes=waybill["file_bytes"],
                source_chat_id=update.effective_chat.id if update.effective_chat else None,
                source_user_id=user.id if user else None,
                source_message_id=waybill.get("source_message_id"),
                source_file_id=waybill.get("source_file_id"),
                file_name=waybill.get("file_name"),
                content_type=waybill.get("content_type"),
            )
        create_pickup_report(
            items=[{"bunkerId": item["id"], "billingUnits": item["billing_units"]} for item in log],
            cleanup_status=context.user_data.get("bunker_cleanup_status", "not_required"),
            cleanup_comment=context.user_data.get("bunker_cleanup_comment"),
            driver_source="telegram",
            driver_user_id=user.id if user else "unknown",
            driver_name=user.full_name if user else None,
            site_photos=context.user_data.get("bunker_site_photos", []),
            waybill=waybill,
            waybill_token=waybill_token,
            waybill_missing_reason=context.user_data.get("bunker_waybill_missing_reason"),
        )
    except Exception as error:
        await query.edit_message_text(f"Не удалось сохранить вывоз: {error}")
        return STATE_CONFIRM

    row = build_pickup_sheet_row(log, datetime.now().strftime("%d.%m.%Y"), waybill_token)
    sheet_error = None
    try:
        append_rows([row])
    except Exception as error:
        sheet_error = str(error)
    report = _format_bunker_report(log)
    if sheet_error:
        report += f"\n\n⚠️ Вывоз сохранён, но запись в Google Sheets не выполнена: {sheet_error}"
    await query.edit_message_text(report)
    _clear_report(context)
    return ConversationHandler.END


def get_bunker_conversation_handler() -> ConversationHandler:
    cancel_handler = CallbackQueryHandler(_cancel, pattern=r"^cancel$")
    return ConversationHandler(
        entry_points=[
            CommandHandler("bunker", bunker_start_report),
            CommandHandler("b", bunker_start_report),
            CommandHandler("zayavka", bunker_start_request),
            CommandHandler("z", bunker_start_request),
        ],
        states={
            STATE_BUNKER: [
                CallbackQueryHandler(page_selected, pattern=r"^page:\d+$"),
                CallbackQueryHandler(bunker_selected, pattern=r"^(bunker:.+|done|cancel)$"),
            ],
            STATE_UNITS: [CallbackQueryHandler(units_selected, pattern=r"^(units:(1|1\.5|2)|cancel)$")],
            STATE_CLEANUP: [CallbackQueryHandler(cleanup_selected, pattern=r"^(cleanup:.+|cancel)$")],
            STATE_CLEANUP_COMMENT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, cleanup_comment),
                cancel_handler,
            ],
            STATE_PHOTOS: [
                MessageHandler(filters.PHOTO | filters.Document.IMAGE, photo_received),
                CallbackQueryHandler(photos_finished, pattern=r"^(photos_done|photos_skip|cancel)$"),
            ],
            STATE_WAYBILL: [
                MessageHandler(filters.PHOTO | filters.Document.ALL, waybill_received),
                CallbackQueryHandler(waybill_skipped, pattern=r"^(waybill_skip|cancel)$"),
            ],
            STATE_WAYBILL_REASON: [CallbackQueryHandler(waybill_reason_selected, pattern=r"^waybill_reason:")],
            STATE_CONFIRM: [CallbackQueryHandler(confirm_pickup, pattern=r"^(confirm|cancel)$")],
        },
        fallbacks=[CommandHandler("cancel", _cancel)],
        per_message=False,
        per_chat=False,
        per_user=True,
    )
