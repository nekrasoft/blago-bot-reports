# MAX-бот для приёма отчётов водителя (бункерные отчёты)
from __future__ import annotations

import asyncio
import json
import logging
import mimetypes
import os
import re
import sys
from collections import defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import unquote, urlparse

import aiohttp
from dotenv import load_dotenv
from maxapi import Bot, Dispatcher
from maxapi.context import MemoryContext, State, StatesGroup
from maxapi.types import (
    BotAdded,
    BotRemoved,
    CallbackButton,
    Command,
    MessageCallback,
    MessageCreated,
)
from maxapi.utils.inline_keyboard import InlineKeyboardBuilder

from bunker_report import (
    _bunker_label,
    _format_bunker_report,
    _format_request_report,
    _get_sorted_bunkers,
)
from map_client import (
    build_container_pickup_row,
    format_note_with_bunker_numbers,
    get_bunker_log_entry,
    get_trip_removal_counterparties,
    mark_bunker_filled,
    record_pickup_by_bunker_id,
)
from sheets_client import append_rows
from waybill_files_db import save_waybill_file
from waybill_notes import format_note_with_waybill_token, generate_waybill_token

load_dotenv(Path(__file__).resolve().parent / ".env")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

PAGE_SIZE = 8
NOT_ALLOWED_MSG = "Извините, бот не может работать в этой группе."
OPERATIONS_PATH = Path(__file__).resolve().parent / "data" / "operations.json"


def _get_allowed_chat_ids() -> set[int]:
    """Разрешённые ID чатов из .env. Пустой список — без ограничений."""
    raw = os.environ.get("ALLOWED_CHAT_IDS", "").strip()
    if not raw:
        return set()
    return {int(x.strip()) for x in raw.split(",") if x.strip()}


class BunkerDialog(StatesGroup):
    selecting = State()


class HodkaDialog(StatesGroup):
    selecting = State()
    waiting_count = State()
    waiting_volume = State()
    waiting_cash = State()
    waiting_file = State()


def _build_bunker_keyboard_max(
    page: int = 0, exclude_ids: set | None = None
) -> tuple[object, int, int]:
    """Клавиатура со списком бункеров для MAX.

    Возвращает (markup, page, total_pages).
    """
    bunkers = _get_sorted_bunkers()
    exclude = exclude_ids or set()
    available = [b for b in bunkers if b.get("id") and b.get("id") not in exclude]

    total = len(available)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page = min(page, total_pages - 1) if total_pages > 0 else 0
    start = page * PAGE_SIZE
    chunk = available[start : start + PAGE_SIZE]

    builder = InlineKeyboardBuilder()

    for b in chunk:
        label = _bunker_label(b)
        bid = b.get("id", "")
        if bid:
            builder.row(CallbackButton(text=label, payload=f"bunker:{bid}"))

    nav: list[CallbackButton] = []
    if page > 0:
        nav.append(CallbackButton(text="◀ Пред", payload=f"page:{page - 1}"))
    if page < total_pages - 1:
        nav.append(CallbackButton(text="След ▶", payload=f"page:{page + 1}"))
    if nav:
        builder.row(*nav)

    builder.row(CallbackButton(text="✅ Готово", payload="done"))
    builder.row(CallbackButton(text="Отмена", payload="cancel"))

    return builder.as_markup(), page, total_pages


def _counterparty_title(item: dict) -> str:
    return str(item.get("shortName") or item.get("name") or "").strip()


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
    cash_income: str | None = None,
) -> dict:
    """Строка для таблицы: ходка/рейс (trip_removal)."""
    try:
        dt = datetime.strptime(date_str, "%d.%m.%Y")
    except ValueError:
        dt = datetime.now()

    op = _load_trip_operation()
    structure = "ФЛ - Вывоз мусора" if _is_private_contractor(contractor) else op.get(
        "структура",
        "ЮЛ - Вывоз мусора",
    )
    row = {
        "Дата": date_str,
        "Месяц": str(dt.month),
        "Структура": structure,
        "КСП": op.get("ксп", "1201"),
        "Операция": op.get("операция", "Поступление по основной деятельности"),
        "КСЗ": op.get("ксз", "1001"),
        "Контрагент": contractor,
        "Примечание": note,
        "Объект": str(trips_count),
    }
    if cash_income:
        row["Приход"] = cash_income
        row["Выручка"] = ""
        row["_skip_formula_columns"] = ["Выручка"]
    return row


def _parse_trips_count(text: str) -> int | None:
    match = re.search(r"\d+", text or "")
    if not match:
        return None
    value = int(match.group(0))
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


def _is_private_contractor(contractor: str) -> bool:
    return contractor.strip().casefold() == "частник"


def _build_hodka_keyboard_max(
    counterparties: list[dict], page: int = 0
) -> tuple[object, int, int]:
    """Клавиатура выбора контрагента для /h (operation_type=trip_removal)."""
    total = len(counterparties)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page = min(page, total_pages - 1) if total_pages > 0 else 0
    start = page * PAGE_SIZE
    chunk = counterparties[start : start + PAGE_SIZE]

    builder = InlineKeyboardBuilder()

    for i, item in enumerate(chunk, start=start):
        title = _counterparty_title(item)
        if title:
            builder.row(CallbackButton(text=title, payload=f"hctr:{i}"))

    nav: list[CallbackButton] = []
    if page > 0:
        nav.append(CallbackButton(text="◀ Пред", payload=f"hpage:{page - 1}"))
    if page < total_pages - 1:
        nav.append(CallbackButton(text="След ▶", payload=f"hpage:{page + 1}"))
    if nav:
        builder.row(*nav)

    builder.row(CallbackButton(text="Отмена", payload="hcancel"))
    return builder.as_markup(), page, total_pages


WAYBILL_MAX_FILE_SIZE_BYTES_DEFAULT = 10 * 1024 * 1024
WAYBILL_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".tif", ".tiff"}


def _build_waybill_keyboard_max() -> object:
    builder = InlineKeyboardBuilder()
    builder.row(CallbackButton(text="Пропустить путевой лист", payload="hfile_skip"))
    builder.row(CallbackButton(text="Отмена", payload="hcancel"))
    return builder.as_markup()


def _build_volume_keyboard_max() -> object:
    builder = InlineKeyboardBuilder()
    builder.row(
        CallbackButton(text="30 м3", payload="hvol:30"),
        CallbackButton(text="36 м3", payload="hvol:36"),
    )
    builder.row(CallbackButton(text="Отмена", payload="hcancel"))
    return builder.as_markup()


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


def _waybill_extension_for_content_type(content_type: str | None) -> str:
    content_type = (content_type or "").split(";", 1)[0].lower().strip()
    if content_type == "application/pdf":
        return ".pdf"
    if content_type == "image/jpeg":
        return ".jpg"
    if content_type == "image/png":
        return ".png"
    if content_type == "image/webp":
        return ".webp"
    if content_type == "image/gif":
        return ".gif"
    guessed = mimetypes.guess_extension(content_type or "")
    return guessed or ""


def _attachment_type_text(attachment: object) -> str:
    raw = getattr(attachment, "type", "") or ""
    value = getattr(raw, "value", raw)
    return str(value).strip().lower()


def _attachment_payload_url(attachment: object) -> str:
    payload = getattr(attachment, "payload", None)
    if isinstance(payload, dict):
        return str(payload.get("url") or "").strip()
    return str(getattr(payload, "url", "") or "").strip()


def _attachment_payload_token(attachment: object) -> str:
    payload = getattr(attachment, "payload", None)
    if isinstance(payload, dict):
        return str(payload.get("token") or "").strip()
    return str(getattr(payload, "token", "") or "").strip()


def _attachment_file_name(attachment: object, url: str) -> str:
    payload = getattr(attachment, "payload", None)
    payload_get = (
        payload.get if isinstance(payload, dict) else lambda key, default=None: default
    )
    file_name = str(
        getattr(attachment, "filename", "")
        or getattr(attachment, "file_name", "")
        or getattr(attachment, "name", "")
        or payload_get("filename")
        or payload_get("file_name")
        or payload_get("name")
        or ""
    ).strip()
    if file_name:
        return Path(file_name).name
    if _attachment_type_text(attachment) == "image":
        return ""
    parsed_path = Path(unquote(urlparse(url).path))
    url_file_name = parsed_path.name
    return url_file_name if Path(url_file_name).suffix else ""


def _normalize_waybill_file_name(
    file_name: str,
    content_type: str | None,
    file_bytes: bytes,
    file_name_seed: str,
) -> str:
    suffix = Path(file_name).suffix
    if suffix:
        return Path(file_name).name

    extension = _waybill_extension_for_content_type(content_type)
    token = re.sub(r"[^A-Za-z0-9_-]+", "", file_name_seed or "")[:24]
    if not token:
        import hashlib

        token = hashlib.sha256(file_bytes).hexdigest()[:12]
    return f"waybill_{token}{extension}"


def _select_max_waybill_attachment(attachments: list[object]) -> object | None:
    for attachment in attachments:
        type_text = _attachment_type_text(attachment)
        url = _attachment_payload_url(attachment)
        if not url:
            continue
        file_name = _attachment_file_name(attachment, url)
        if type_text == "image":
            return attachment
        if type_text == "file" and _is_supported_waybill_type(file_name, None):
            return attachment
    return None


async def _download_max_attachment_bytes(
    url: str,
    token: str,
    max_size: int,
) -> tuple[bytes, str]:
    headers = {"Authorization": f"Bearer {token}"} if token else None
    chunks: list[bytes] = []
    total_size = 0

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                raise ValueError("Не удалось скачать файл из MAX. Попробуйте отправить его ещё раз.")

            content_length = response.headers.get("Content-Length")
            if content_length:
                try:
                    declared_size = int(content_length)
                except ValueError:
                    declared_size = 0
                if declared_size > max_size:
                    raise ValueError(f"Файл слишком большой. Лимит: {max_size // (1024 * 1024)} МБ.")

            async for chunk in response.content.iter_chunked(64 * 1024):
                total_size += len(chunk)
                if total_size > max_size:
                    raise ValueError(f"Файл слишком большой. Лимит: {max_size // (1024 * 1024)} МБ.")
                chunks.append(chunk)

            return b"".join(chunks), response.headers.get("Content-Type", "")


async def _download_max_waybill(message) -> dict:
    body = getattr(message, "body", None)
    attachments = list(getattr(body, "attachments", None) or [])
    attachment = _select_max_waybill_attachment(attachments)
    if attachment is None:
        raise ValueError("Загрузите картинку или PDF с путевым листом.")

    max_size = _get_waybill_max_file_size_bytes()
    url = _attachment_payload_url(attachment)
    file_size = int(getattr(attachment, "size", 0) or 0)
    if file_size and file_size > max_size:
        raise ValueError(f"Файл слишком большой. Лимит: {max_size // (1024 * 1024)} МБ.")

    file_name = _attachment_file_name(attachment, url)
    token = _attachment_payload_token(attachment)
    file_bytes, response_content_type = await _download_max_attachment_bytes(url, token, max_size)

    content_type = (
        mimetypes.guess_type(file_name)[0] or response_content_type.split(";", 1)[0]
    )
    detected_content_type = _detect_waybill_content_type(file_name, content_type, file_bytes)
    if not _is_supported_waybill_type(file_name, detected_content_type):
        raise ValueError("Можно загрузить только картинку или PDF.")
    source_file_id = token or url
    file_name = _normalize_waybill_file_name(
        file_name,
        detected_content_type,
        file_bytes,
        token,
    )

    return {
        "file_bytes": file_bytes,
        "file_name": file_name,
        "content_type": detected_content_type,
        "source_file_id": source_file_id,
    }


def _max_chat_id(event: MessageCreated) -> int | None:
    value = getattr(event, "chat_id", None)
    if value is not None:
        return value
    recipient = getattr(event.message, "recipient", None)
    return getattr(recipient, "chat_id", None)


def _max_user_id(event: MessageCreated) -> int | None:
    sender = getattr(event.message, "sender", None)
    return getattr(sender, "user_id", None)


def _max_message_id(event: MessageCreated) -> str | None:
    body = getattr(event.message, "body", None)
    return getattr(body, "mid", None)


async def _append_hodka_report_max(
    event: MessageCreated | MessageCallback,
    context: MemoryContext,
    *,
    waybill_token: str | None,
) -> None:
    data = await context.get_data()
    contractor = str(data.get("hodka_selected_contractor") or "").strip()
    trips_count = data.get("hodka_trips_count")
    volume_note = str(data.get("hodka_volume_note") or "").strip()
    cash_income = str(data.get("hodka_cash_income") or "").strip()
    date_str = str(data.get("hodka_date_str") or "").strip()
    if (
        not contractor
        or not trips_count
        or not volume_note
        or not date_str
        or (_is_private_contractor(contractor) and not cash_income)
    ):
        await context.clear()
        await event.message.answer("Данные отчёта устарели. Запустите /h заново.")
        return

    note = format_note_with_waybill_token(volume_note, waybill_token) if waybill_token else volume_note
    row = _build_trip_row(
        contractor,
        int(trips_count),
        date_str,
        note,
        cash_income=cash_income or None,
    )

    try:
        append_rows([row])
    except Exception as e:
        await event.message.answer(f"Ошибка записи в таблицу: {e}")
        return

    await context.clear()
    if _is_private_contractor(contractor):
        suffix = ""
    else:
        suffix = " Путевой лист принят." if waybill_token else " Путевой лист пропущен."
    cash_text = f", наличка: {cash_income}" if cash_income else ""
    await event.message.answer(
        text=f"Записано: {contractor}, ходок: {trips_count}, {volume_note}{cash_text}.{suffix}"
    )


bot = Bot(token=os.environ.get("MAX_BOT_TOKEN", ""))
dp = Dispatcher()


@dp.bot_added()
async def handle_bot_added(event: BotAdded) -> None:
    """Добавление бота в чат — проверка whitelist."""
    logger.info("Бот добавлен в чат %s", event.chat_id)
    allowed = _get_allowed_chat_ids()
    if allowed and event.chat_id not in allowed:
        try:
            await bot.send_message(chat_id=event.chat_id, text=NOT_ALLOWED_MSG)
        except Exception:
            pass
        try:
            await bot.delete_me_from_chat(chat_id=event.chat_id)
        except Exception as e:
            logger.warning("Не удалось выйти из чата %s: %s", event.chat_id, e)


@dp.bot_removed()
async def handle_bot_removed(event: BotRemoved) -> None:
    """Удаление бота из чата."""
    logger.info("Бот удалён из чата %s", event.chat_id)


@dp.message_created(Command("bunker"))
@dp.message_created(Command("b"))
async def handle_bunker_report(event: MessageCreated, context: MemoryContext) -> None:
    """Команда /bunker или /b — отчёт о вывозе."""
    await _start_bunker_dialog(event, context, mode="report")


@dp.message_created(Command("zayavka"))
@dp.message_created(Command("z"))
async def handle_bunker_request(event: MessageCreated, context: MemoryContext) -> None:
    """Команда /zayavka или /z — заявка на опустошение."""
    await _start_bunker_dialog(event, context, mode="request")


@dp.message_created(Command("h"))
async def handle_hodka_start(event: MessageCreated, context: MemoryContext) -> None:
    """Команда /h — ходка/рейс: выбор trip_removal-контрагента и ввод количества."""
    counterparties = [
        c for c in get_trip_removal_counterparties() if _counterparty_title(c)
    ]
    if not counterparties:
        await event.message.answer(
            "Не найдено контрагентов с operation_type=trip_removal."
        )
        return

    await context.set_state(HodkaDialog.selecting)
    await context.update_data(
        hodka_counterparties=counterparties,
        hodka_selected_contractor="",
        hodka_page=0,
        hodka_trips_count=None,
        hodka_volume_note="",
        hodka_cash_income="",
        hodka_date_str="",
        hodka_waybill_token="",
    )

    markup, _, _ = _build_hodka_keyboard_max(counterparties, 0)
    prompt = "Команда /h (ходка/рейс).\nВыберите контрагента, для которого были сделаны ходки:"
    await event.message.answer(text=prompt, attachments=[markup])


async def _start_bunker_dialog(
    event: MessageCreated, context: MemoryContext, mode: str
) -> None:
    """Общая логика старта диалога выбора бункеров."""
    bunkers = _get_sorted_bunkers()
    if not bunkers:
        await event.message.answer("Бункеры не найдены. Проверьте настройку MAP_SERVICE_URL.")
        return

    await context.set_state(BunkerDialog.selecting)
    await context.update_data(mode=mode, page=0, bunker_log=[], selected_ids=[])

    markup, _, _ = _build_bunker_keyboard_max(0, set())

    if mode == "request":
        prompt = "Выберите бункер для заявки на опустошение (или несколько по очереди):"
    else:
        prompt = "Выберите опустошённый бункер (или несколько по очереди):"

    await event.message.answer(text=prompt, attachments=[markup])


@dp.message_callback(BunkerDialog.selecting)
async def handle_bunker_callback(event: MessageCallback, context: MemoryContext) -> None:
    """Обработка всех нажатий кнопок в диалоге выбора бункеров."""
    payload = event.callback.payload or ""

    if payload == "cancel":
        await _callback_cancel(event, context)
    elif payload == "done":
        await _callback_done(event, context)
    elif payload.startswith("page:"):
        await _callback_page(event, context, payload)
    elif payload.startswith("bunker:"):
        await _callback_bunker(event, context, payload)


async def _callback_cancel(event: MessageCallback, context: MemoryContext) -> None:
    """Отмена выбора бункеров."""
    await context.clear()
    await event.message.delete()
    await event.message.answer(text="Отменено.")


async def _callback_done(event: MessageCallback, context: MemoryContext) -> None:
    """Завершение выбора бункеров."""
    data = await context.get_data()
    bunker_log = data.get("bunker_log", [])
    mode = data.get("mode", "report")
    await context.clear()

    if not bunker_log:
        msg = "Ничего не принято." if mode == "request" else "Ничего не записано."
        await event.answer(new_text=msg)
        return

    if mode == "report":
        date_str = datetime.now().strftime("%d.%m.%Y")
        by_key: dict = defaultdict(list)
        for item in bunker_log:
            key = (item["contractor"], item["note"])
            by_key[key].append(item)
        rows = []
        for (contractor, note), items in sorted(by_key.items()):
            note_with_numbers = format_note_with_bunker_numbers(
                note,
                [item.get("number") for item in items],
            )
            rows.append(
                build_container_pickup_row(
                    contractor,
                    note_with_numbers,
                    len(items),
                    date_str,
                )
            )
        try:
            append_rows(rows)
        except Exception as e:
            await event.message.answer(f"Ошибка записи в таблицу: {e}")
        report = _format_bunker_report(bunker_log)
    else:
        report = _format_request_report(bunker_log)

    await event.message.delete()
    await event.message.answer(text=report)


async def _callback_page(
    event: MessageCallback, context: MemoryContext, payload: str
) -> None:
    """Переключение страницы списка бункеров."""
    try:
        page = int(payload.replace("page:", "", 1))
    except ValueError:
        page = 0

    data = await context.get_data()
    await context.update_data(page=page)

    selected_ids = set(data.get("selected_ids", []))
    mode = data.get("mode", "report")
    bunker_log = data.get("bunker_log", [])
    prefix = "Принято:" if mode == "request" else "Записано:"

    available = [b for b in _get_sorted_bunkers() if b.get("id") and b.get("id") not in selected_ids]
    total_pages = max(1, (len(available) + PAGE_SIZE - 1) // PAGE_SIZE)
    text = f"Стр. {page + 1}/{total_pages}. Выберите бункер:"

    if bunker_log:
        preview = [f"• {_bunker_label(x)}" for x in bunker_log[-3:]]
        text = f"{prefix}\n" + "\n".join(preview) + "\n\n" + text

    markup, _, _ = _build_bunker_keyboard_max(page, selected_ids)
    await event.message.delete()
    await event.message.answer(text=text, attachments=[markup])


async def _callback_bunker(
    event: MessageCallback, context: MemoryContext, payload: str
) -> None:
    """Выбор конкретного бункера из списка."""
    bunker_id = payload.replace("bunker:", "", 1)
    if not bunker_id:
        return

    data = await context.get_data()
    mode = data.get("mode", "report")
    page = data.get("page", 0)
    bunker_log: list[dict] = data.get("bunker_log", [])
    selected_ids: set[str] = set(data.get("selected_ids", []))

    if mode == "report":
        date_str = datetime.now().strftime("%d.%m.%Y")
        row, map_ok = record_pickup_by_bunker_id(bunker_id, date_str, 1)
        if not row:
            await event.message.answer("Ошибка: бункер не найден.")
            return
        log_entry = get_bunker_log_entry(bunker_id)
        if not log_entry:
            log_entry = {
                "contractor": row.get("Контрагент", ""),
                "note": row.get("Примечание", ""),
                "number": "?",
                "address": "",
            }
    else:
        log_entry = get_bunker_log_entry(bunker_id)
        if not log_entry:
            await event.message.answer("Ошибка: бункер не найден.")
            return
        map_ok = mark_bunker_filled(bunker_id)

    bunker_log.append(log_entry)
    selected_ids.add(bunker_id)
    await context.update_data(bunker_log=bunker_log, selected_ids=list(selected_ids))

    if mode == "request":
        answer_txt = "Принято, карта обновлена ✓" if map_ok else "Принято ✓"
    else:
        answer_txt = "Записано, карта обновлена ✓" if map_ok else "Записано ✓"

    preview = [f"• {_bunker_label(x)}" for x in bunker_log[-5:]]
    prompt_suffix = f"{answer_txt}\n\nВыберите ещё бункер или Готово:\n\n" + "\n".join(preview)
    markup, _, _ = _build_bunker_keyboard_max(page, selected_ids)
    await event.message.delete()
    await event.message.answer(text=prompt_suffix, attachments=[markup])


@dp.message_callback(HodkaDialog.selecting)
async def handle_hodka_callback(event: MessageCallback, context: MemoryContext) -> None:
    """Обработка кнопок выбора контрагента для /h."""
    payload = event.callback.payload or ""
    data = await context.get_data()
    counterparties = data.get("hodka_counterparties", [])
    page = int(data.get("hodka_page", 0) or 0)

    if payload == "hcancel":
        await context.clear()
        await event.message.delete()
        await event.message.answer(text="Отменено.")
        return

    if not isinstance(counterparties, list) or not counterparties:
        await context.clear()
        await event.message.delete()
        await event.message.answer(text="Список контрагентов пуст. Запустите /h заново.")
        return

    if payload.startswith("hpage:"):
        try:
            page = int(payload.replace("hpage:", "", 1))
        except ValueError:
            page = 0
        await context.update_data(hodka_page=page)
        markup, page, total_pages = _build_hodka_keyboard_max(counterparties, page)
        text = f"Стр. {page + 1}/{total_pages}. Выберите контрагента с operation_type=trip_removal:"
        await event.message.delete()
        await event.message.answer(text=text, attachments=[markup])
        return

    if not payload.startswith("hctr:"):
        return

    try:
        idx = int(payload.replace("hctr:", "", 1))
    except ValueError:
        await event.message.answer("Некорректный выбор. Попробуйте снова.")
        return

    if idx < 0 or idx >= len(counterparties):
        await context.clear()
        await event.message.delete()
        await event.message.answer("Список устарел. Запустите /h заново.")
        return

    contractor = _counterparty_title(counterparties[idx])
    if not contractor:
        await event.message.answer("Контрагент не найден. Попробуйте снова.")
        return

    await context.set_state(HodkaDialog.waiting_count)
    await context.update_data(hodka_selected_contractor=contractor)

    await event.message.delete()
    await event.message.answer(
        text=f"Контрагент: {contractor}\nВведите количество ходок (целое число):"
    )


@dp.message_created(HodkaDialog.waiting_count)
async def handle_hodka_count(event: MessageCreated, context: MemoryContext) -> None:
    """Приём количества ходок и переход к выбору объёма."""
    body = getattr(event.message, "body", None)
    text = str(getattr(body, "text", "") or "").strip()
    if text.lower() in {"отмена", "cancel", "/cancel"}:
        await context.clear()
        await event.message.answer("Отменено.")
        return
    if not text:
        await event.message.answer("Введите число ходок, например: 2")
        return

    trips_count = _parse_trips_count(text)
    if not trips_count:
        await event.message.answer("Введите положительное целое число, например: 2")
        return

    data = await context.get_data()
    contractor = str(data.get("hodka_selected_contractor") or "").strip()
    if not contractor:
        await context.clear()
        await event.message.answer("Контрагент не выбран. Запустите /h заново.")
        return

    date_str = datetime.now().strftime("%d.%m.%Y")
    await context.set_state(HodkaDialog.waiting_volume)
    await context.update_data(
        hodka_trips_count=trips_count,
        hodka_date_str=date_str,
        hodka_waybill_token=generate_waybill_token(),
    )
    await event.message.answer(
        text="Выберите кузов или введите общий объём вывезенного мусора вручную:",
        attachments=[_build_volume_keyboard_max()],
    )


async def _ask_waybill_max(event: MessageCreated | MessageCallback, context: MemoryContext) -> None:
    data = await context.get_data()
    if not str(data.get("hodka_waybill_token") or "").strip():
        await context.update_data(hodka_waybill_token=generate_waybill_token())
    await context.set_state(HodkaDialog.waiting_file)
    await event.message.answer(
        text=(
            "Загрузите путевой лист: фото или PDF подписанной мастером бумаги.\n"
            "Если путевого листа нет, нажмите «Пропустить путевой лист»."
        ),
        attachments=[_build_waybill_keyboard_max()],
    )


async def _ask_cash_or_waybill_max(
    event: MessageCreated | MessageCallback,
    context: MemoryContext,
) -> None:
    data = await context.get_data()
    contractor = str(data.get("hodka_selected_contractor") or "").strip()
    if not _is_private_contractor(contractor):
        await _ask_waybill_max(event, context)
        return

    await context.set_state(HodkaDialog.waiting_cash)
    await event.message.answer(text="Введите сумму полученной налички:")


@dp.message_callback(HodkaDialog.waiting_volume)
async def handle_hodka_volume_callback(event: MessageCallback, context: MemoryContext) -> None:
    """Обработка кнопок выбора объёма для /h."""
    payload = event.callback.payload or ""
    if payload == "hcancel":
        await context.clear()
        await event.message.delete()
        await event.message.answer(text="Отменено.")
        return
    if not payload.startswith("hvol:"):
        return

    data = await context.get_data()
    trips_count = data.get("hodka_trips_count")
    if not trips_count:
        await context.clear()
        await event.message.delete()
        await event.message.answer("Данные отчёта устарели. Запустите /h заново.")
        return

    body_volume = _parse_volume(payload.replace("hvol:", "", 1))
    if body_volume is None:
        await event.message.answer("Некорректный объём. Попробуйте снова.")
        return

    total_volume = body_volume * int(trips_count)
    await context.update_data(hodka_volume_note=_volume_note(total_volume))
    await event.message.delete()
    await event.message.answer(
        text=f"Объём: {_format_volume(total_volume)} м3 ({_format_volume(body_volume)} м3 × {trips_count} ход.)"
    )
    await _ask_cash_or_waybill_max(event, context)


@dp.message_created(HodkaDialog.waiting_volume)
async def handle_hodka_volume(event: MessageCreated, context: MemoryContext) -> None:
    """Приём ручного итогового объёма для /h."""
    body = getattr(event.message, "body", None)
    text = str(getattr(body, "text", "") or "").strip()
    if text.lower() in {"отмена", "cancel", "/cancel"}:
        await context.clear()
        await event.message.answer("Отменено.")
        return

    volume = _parse_volume(text)
    if volume is None:
        await event.message.answer(
            "Выберите кузов кнопкой или введите общий объём, например: 10",
            attachments=[_build_volume_keyboard_max()],
        )
        return

    await context.update_data(hodka_volume_note=_volume_note(volume))
    await event.message.answer(text=f"Объём: {_format_volume(volume)} м3")
    await _ask_cash_or_waybill_max(event, context)


@dp.message_created(HodkaDialog.waiting_cash)
async def handle_hodka_cash(event: MessageCreated, context: MemoryContext) -> None:
    """Приём суммы налички для частника."""
    body = getattr(event.message, "body", None)
    text = str(getattr(body, "text", "") or "").strip()
    if text.lower() in {"отмена", "cancel", "/cancel"}:
        await context.clear()
        await event.message.answer("Отменено.")
        return

    amount = _parse_volume(text)
    if amount is None:
        await event.message.answer("Введите сумму полученной налички, например: 10000")
        return

    await context.update_data(hodka_cash_income=_format_volume(amount))
    await event.message.answer(text=f"Наличка: {_format_volume(amount)}")
    await _append_hodka_report_max(event, context, waybill_token=None)


@dp.message_callback(HodkaDialog.waiting_file)
async def handle_hodka_file_callback(event: MessageCallback, context: MemoryContext) -> None:
    """Обработка кнопок на этапе загрузки путевого листа."""
    payload = event.callback.payload or ""
    if payload == "hcancel":
        await context.clear()
        await event.message.delete()
        await event.message.answer(text="Отменено.")
        return
    if payload != "hfile_skip":
        return

    await event.message.delete()
    await event.message.answer(text="Путевой лист пропущен.")
    await _append_hodka_report_max(event, context, waybill_token=None)


@dp.message_created(HodkaDialog.waiting_file)
async def handle_hodka_file(event: MessageCreated, context: MemoryContext) -> None:
    """Приём путевого листа или отказа от загрузки."""
    body = getattr(event.message, "body", None)
    text = str(getattr(body, "text", "") or "").strip().lower()
    if text in {"отмена", "cancel", "/cancel"}:
        await context.clear()
        await event.message.answer("Отменено.")
        return
    if text in {"нет", "не", "no", "skip", "пропустить", "без файла"}:
        await _append_hodka_report_max(event, context, waybill_token=None)
        return

    attachments = list(getattr(body, "attachments", None) or [])
    if not attachments:
        await event.message.answer(
            text="Отправьте фото или PDF путевого листа либо нажмите «Пропустить путевой лист».",
            attachments=[_build_waybill_keyboard_max()],
        )
        return

    data = await context.get_data()
    token = str(data.get("hodka_waybill_token") or "").strip()
    if not token:
        token = generate_waybill_token()
        await context.update_data(hodka_waybill_token=token)

    try:
        file_info = await _download_max_waybill(event.message)
        save_waybill_file(
            file_token=token,
            source="max",
            file_bytes=file_info["file_bytes"],
            source_chat_id=_max_chat_id(event),
            source_user_id=_max_user_id(event),
            source_message_id=_max_message_id(event),
            source_file_id=file_info["source_file_id"],
            file_name=file_info["file_name"],
            content_type=file_info["content_type"],
        )
    except ValueError as e:
        await event.message.answer(str(e))
        return
    except Exception as e:
        await event.message.answer(
            "Не удалось сохранить путевой лист в БД. "
            f"Попробуйте отправить файл ещё раз или пропустите его. Ошибка: {e}"
        )
        return

    await _append_hodka_report_max(event, context, waybill_token=token)


def main() -> None:
    """Запуск MAX-бота."""
    token = os.environ.get("MAX_BOT_TOKEN")
    if not token:
        raise ValueError("Задайте MAX_BOT_TOKEN в .env")
    asyncio.run(dp.start_polling(bot))


if __name__ == "__main__":
    main()
