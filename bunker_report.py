# Интерактивный отчёт по бункеру: /bunker, /report, /zayavka

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

from map_client import (
    FILL_LEVEL_REQUEST,
    build_container_pickup_row,
    get_all_bunkers,
    get_bunker_log_entry,
    record_pickup_by_bunker_id,
    set_bunker_fill_level,
)
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


def _format_bunker_report(log: list[dict]) -> str:
    """Форматирование отчёта по вывозу контейнеров для публикации в чат."""
    date_str = datetime.now().strftime("%d.%m.%Y")
    lines = ["📋 Отчёт по вывозу контейнеров", f"Дата: {date_str}", ""]

    for item in log:
        contractor = item.get("contractor", "")
        num = item.get("number", "?")
        addr = _address_without_city(item.get("address", ""))
        lines.append(f"• {contractor} — №{num}, {addr}")

    lines.extend(["", f"Всего: {len(log)} контейнер(ов)"])
    return "\n".join(lines)


def _format_request_report(log: list[dict]) -> str:
    """Форматирование заявки на опустошение для публикации в чат."""
    date_str = datetime.now().strftime("%d.%m.%Y")
    lines = ["✅ Заявка принята", f"Дата: {date_str}", ""]

    for item in log:
        contractor = item.get("contractor", "")
        num = item.get("number", "?")
        addr = _address_without_city(item.get("address", ""))
        lines.append(f"• {contractor} — №{num}, {addr}")

    lines.extend(["", f"Всего: {len(log)} контейнер(ов)"])
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


async def _bunker_start_impl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Общая логика старта выбора бункеров."""
    if not update.message:
        return ConversationHandler.END

    context.user_data["bunker_page"] = 0
    context.user_data["bunker_log"] = []
    context.user_data["bunker_selected_ids"] = set()
    # Режим задаётся в entry point: "report" или "request"
    mode = context.user_data.get("bunker_mode", "report")

    bunkers = _get_sorted_bunkers()
    if not bunkers:
        await update.message.reply_text("Бункеры не найдены. Проверьте настройку MAP_SERVICE_URL.")
        return ConversationHandler.END

    if mode == "request":
        prompt = "Выберите бункер для заявки на опустошение (или несколько по очереди):"
    else:
        prompt = "Выберите опустошённый бункер (или несколько по очереди):"

    await update.message.reply_text(prompt, reply_markup=_build_bunker_keyboard(0, set()))
    return STATE_BUNKER


async def bunker_start_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Команда /bunker или /report — отчёт о вывозе."""
    context.user_data["bunker_mode"] = "report"
    return await _bunker_start_impl(update, context)


async def bunker_start_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Команда /zayavka — заявка на опустошение (fillLevel=100%)."""
    context.user_data["bunker_mode"] = "request"
    return await _bunker_start_impl(update, context)


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

    mode = context.user_data.get("bunker_mode", "report")
    prefix = "Принято:" if mode == "request" else "Записано:"

    text = f"Стр. {page + 1}/{total_pages}. Выберите бункер:"
    log = context.user_data.get("bunker_log", [])
    if log:
        preview = [
            "• {c} — №{n}, {a}".format(c=x.get("contractor", ""), n=x.get("number", "?"), a=_address_without_city(x.get("address", "")))
            for x in log[-3:]
        ]
        text = f"{prefix}\n" + "\n".join(preview) + "\n\n" + text

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
        mode = context.user_data.get("bunker_mode", "report")

        if log:
            chat_id = update.effective_chat.id if update.effective_chat else None

            if mode == "report":
                # Запись в таблицу
                date_str = datetime.now().strftime("%d.%m.%Y")
                by_key = defaultdict(list)
                for item in log:
                    key = (item["contractor"], item["note"])
                    by_key[key].append(item)
                rows = [
                    build_container_pickup_row(contractor, note, len(items), date_str)
                    for (contractor, note), items in sorted(by_key.items())
                ]
                try:
                    append_rows(rows)
                except Exception as e:
                    if chat_id:
                        await context.bot.send_message(chat_id=chat_id, text=f"Ошибка записи в таблицу: {e}")
                report = _format_bunker_report(log)
            else:
                report = _format_request_report(log)

            await query.edit_message_text(report)
        else:
            msg = "Ничего не принято." if mode == "request" else "Ничего не записано."
            await query.edit_message_text(msg)
        return ConversationHandler.END

    if not data.startswith("bunker:"):
        return STATE_BUNKER

    bunker_id = data.replace("bunker:", "", 1)
    if not bunker_id:
        return STATE_BUNKER

    mode = context.user_data.get("bunker_mode", "report")

    if mode == "report":
        date_str = datetime.now().strftime("%d.%m.%Y")
        row, map_ok = record_pickup_by_bunker_id(bunker_id, date_str, 1)
        if not row:
            await query.answer("Ошибка: бункер не найден.", show_alert=True)
            return STATE_BUNKER
        log_entry = get_bunker_log_entry(bunker_id)
        if not log_entry:
            log_entry = {"contractor": row.get("Контрагент", ""), "note": row.get("Примечание", ""), "number": "?", "address": ""}
    else:
        log_entry = get_bunker_log_entry(bunker_id)
        if not log_entry:
            await query.answer("Ошибка: бункер не найден.", show_alert=True)
            return STATE_BUNKER
        map_ok = set_bunker_fill_level(bunker_id, FILL_LEVEL_REQUEST)

    if "bunker_log" not in context.user_data:
        context.user_data["bunker_log"] = []
    context.user_data["bunker_log"].append(log_entry)
    selected_ids = context.user_data.get("bunker_selected_ids", set())
    selected_ids.add(bunker_id)

    if mode == "request":
        answer_txt = f"Принято, карта обновлена ✓" if map_ok else "Принято ✓"
    else:
        answer_txt = f"Записано, карта обновлена ✓" if map_ok else "Записано ✓"
    await query.answer(answer_txt, show_alert=False)

    page = context.user_data.get("bunker_page", 0)
    preview = [
        "• {c} — №{n}, {a}".format(c=x.get("contractor", ""), n=x.get("number", "?"), a=_address_without_city(x.get("address", "")))
        for x in context.user_data["bunker_log"][-5:]
    ]
    prompt_suffix = "Выберите ещё бункер или Готово:\n\n" + "\n".join(preview)
    await query.edit_message_text(prompt_suffix, reply_markup=_build_bunker_keyboard(page, selected_ids))
    return STATE_BUNKER


def get_bunker_conversation_handler() -> ConversationHandler:
    return ConversationHandler(
        entry_points=[
            CommandHandler("bunker", bunker_start_report),
            CommandHandler("b", bunker_start_report),
            CommandHandler("zayavka", bunker_start_request),
            CommandHandler("z", bunker_start_request),
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
