# MAX-бот для приёма отчётов водителя (бункерные отчёты)
from __future__ import annotations

import asyncio
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

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
    FILL_LEVEL_REQUEST,
    build_container_pickup_row,
    get_bunker_log_entry,
    record_pickup_by_bunker_id,
    set_bunker_fill_level,
)
from sheets_client import append_rows

load_dotenv(Path(__file__).resolve().parent / ".env")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

PAGE_SIZE = 8
NOT_ALLOWED_MSG = "Извините, бот не может работать в этой группе."


def _get_allowed_chat_ids() -> set[int]:
    """Разрешённые ID чатов из .env. Пустой список — без ограничений."""
    raw = os.environ.get("ALLOWED_CHAT_IDS", "").strip()
    if not raw:
        return set()
    return {int(x.strip()) for x in raw.split(",") if x.strip()}


class BunkerDialog(StatesGroup):
    selecting = State()


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
        rows = [
            build_container_pickup_row(contractor, note, len(items), date_str)
            for (contractor, note), items in sorted(by_key.items())
        ]
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
        map_ok = set_bunker_fill_level(bunker_id, FILL_LEVEL_REQUEST)

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


def main() -> None:
    """Запуск MAX-бота."""
    token = os.environ.get("MAX_BOT_TOKEN")
    if not token:
        raise ValueError("Задайте MAX_BOT_TOKEN в .env")
    asyncio.run(dp.start_polling(bot))


if __name__ == "__main__":
    main()
