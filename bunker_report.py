# Интерактивный отчёт по бункеру: /bunker, /report

from __future__ import annotations

from collections import defaultdict
from datetime import datetime

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
)

from map_client import get_all_bunkers, record_pickup_by_bunker_id
from sheets_client import append_rows

STATE_BUNKER = 0
PAGE_SIZE = 8
CANCEL = "cancel"


def _get_sorted_bunkers() -> list[dict]:
    """Все бункеры, отсортированные по контрагенту и адресу."""
    bunkers = get_all_bunkers()
    return sorted(
        bunkers,
        key=lambda b: (
            b.get("contractor", ""),
            b.get("address", ""),
            b.get("number", ""),
        ),
    )


def _bunker_label(b: dict, max_len: int = 50) -> str:
    """Краткая подпись для кнопки: №12 · Контрагент · Адрес."""
    num = b.get("number", "?")
    contractor = (b.get("contractor") or "").strip()
    addr = (b.get("address") or "").strip()
    if "," in addr:
        addr = addr.split(",", 1)[1].strip()
    parts = [f"№{num}", contractor, addr]
    label = " · ".join(p for p in parts if p)
    return label[:max_len] + ("…" if len(label) > max_len else "")


def _format_bunker_report(log: list[dict]) -> str:
    """Форматирование отчёта по вывозу контейнеров для публикации в чат."""
    by_contractor = defaultdict(list)
    for item in log:
        by_contractor[item["contractor"]].append(item["note"])

    date_str = datetime.now().strftime("%d.%m.%Y")
    lines = ["📋 Отчёт по вывозу контейнеров", f"Дата: {date_str}", ""]

    total = 0
    for contractor, notes in sorted(by_contractor.items()):
        count = len(notes)
        total += count
        # Без дублей адресов: несколько бункеров по одному адресу — адрес один раз
        unique_notes = list(dict.fromkeys(notes))
        notes_str = ", ".join(unique_notes)
        lines.append(f"• {contractor} — {count} шт. ({notes_str})")

    lines.extend(["", f"Всего: {total} контейнер(ов)"])
    return "\n".join(lines)


def _build_bunker_keyboard(page: int = 0, exclude_ids: set | frozenset | None = None) -> InlineKeyboardMarkup:
    """Клавиатура со списком бункеров. exclude_ids — уже выбранные (скрываются)."""
    bunkers = _get_sorted_bunkers()
    exclude = exclude_ids or set()
    available = [b for b in bunkers if b.get("id") and b.get("id") not in exclude]

    total = len(available)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page = min(page, total_pages - 1) if total_pages > 0 else 0
    start = page * PAGE_SIZE
    chunk = available[start : start + PAGE_SIZE]

    buttons = []
    for b in chunk:
        label = _bunker_label(b)
        bid = b.get("id", "")
        if bid:
            buttons.append([InlineKeyboardButton(label, callback_data=f"bunker:{bid}")])

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


async def bunker_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Команда /bunker или /report."""
    if not update.message:
        return ConversationHandler.END

    context.user_data["bunker_page"] = 0
    context.user_data["bunker_log"] = []
    context.user_data["bunker_selected_ids"] = set()

    bunkers = _get_sorted_bunkers()
    if not bunkers:
        await update.message.reply_text("Бункеры не найдены. Проверьте настройку MAP_SERVICE_URL.")
        return ConversationHandler.END

    await update.message.reply_text(
        "Выберите опустошённый бункер (или несколько по очереди):",
        reply_markup=_build_bunker_keyboard(0, set()),
    )
    return STATE_BUNKER


async def page_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Переключение страницы списка бункеров."""
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    await query.answer()

    data = query.data or ""
    if not data.startswith("page:"):
        return STATE_BUNKER

    try:
        page = int(data.replace("page:", "", 1))
    except ValueError:
        page = 0

    context.user_data["bunker_page"] = page
    exclude = context.user_data.get("bunker_selected_ids", set())
    available = [b for b in _get_sorted_bunkers() if b.get("id") and b.get("id") not in exclude]
    total_pages = max(1, (len(available) + PAGE_SIZE - 1) // PAGE_SIZE)

    text = f"Стр. {page + 1}/{total_pages}. Выберите бункер:"
    log = context.user_data.get("bunker_log", [])
    if log:
        preview = ["• {c}, {n}".format(c=x["contractor"], n=x["note"]) for x in log[-3:]]
        text = "Записано:\n" + "\n".join(preview) + "\n\n" + text

    await query.edit_message_text(text, reply_markup=_build_bunker_keyboard(page, exclude))
    return STATE_BUNKER


async def bunker_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Выбор бункера: 1 вывоз, сохранение, обновление карты."""
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    await query.answer()

    data = query.data or ""
    if data == CANCEL:
        await query.edit_message_text("Отменено.")
        return ConversationHandler.END
    if data == "done":
        log = context.user_data.get("bunker_log", [])
        if log:
            await query.edit_message_text("Готово.")
            # Публикуем собранный отчёт в чат
            report = _format_bunker_report(log)
            chat_id = update.effective_chat.id if update.effective_chat else None
            if chat_id:
                await context.bot.send_message(chat_id=chat_id, text=report)
        else:
            await query.edit_message_text("Ничего не записано.")
        return ConversationHandler.END

    if not data.startswith("bunker:"):
        return STATE_BUNKER

    bunker_id = data.replace("bunker:", "", 1)
    if not bunker_id:
        return STATE_BUNKER

    date_str = datetime.now().strftime("%d.%m.%Y")
    row, map_ok = record_pickup_by_bunker_id(bunker_id, date_str, 1)
    if not row:
        await query.answer("Ошибка: бункер не найден.", show_alert=True)
        return STATE_BUNKER

    try:
        append_rows([row])
    except Exception as e:
        await query.answer(f"Ошибка записи: {e}", show_alert=True)
        return STATE_BUNKER

    contractor = row.get("Контрагент", "")
    note = row.get("Примечание", "")
    log_entry = {"contractor": contractor, "note": note}
    if "bunker_log" not in context.user_data:
        context.user_data["bunker_log"] = []
    context.user_data["bunker_log"].append(log_entry)
    selected_ids = context.user_data.get("bunker_selected_ids", set())
    selected_ids.add(bunker_id)

    map_txt = ", карта обновлена" if map_ok else ""
    await query.answer(f"Записано{map_txt} ✓", show_alert=False)

    page = context.user_data.get("bunker_page", 0)
    preview = ["• {c}, {n}".format(c=x["contractor"], n=x["note"]) for x in context.user_data["bunker_log"][-5:]]
    text = "Выберите ещё бункер или Готово:\n\n" + "\n".join(preview)
    await query.edit_message_text(text, reply_markup=_build_bunker_keyboard(page, selected_ids))
    return STATE_BUNKER


def get_bunker_conversation_handler() -> ConversationHandler:
    return ConversationHandler(
        entry_points=[
            CommandHandler("bunker", bunker_start),
            CommandHandler("report", bunker_start),
        ],
        states={
            STATE_BUNKER: [
                CallbackQueryHandler(page_selected, pattern=r"^page:\d+"),
                CallbackQueryHandler(bunker_selected, pattern=r"^(bunker:.+|done|cancel)$"),
            ],
        },
        fallbacks=[],
        per_message=False,
        per_chat=False,
        per_user=True,
    )
