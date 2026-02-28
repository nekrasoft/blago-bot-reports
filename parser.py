# Парсер сообщений водителя через OpenAI API
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

from openai import OpenAI

# Пути к конфигурации
PROJECT_ROOT = Path(__file__).resolve().parent
COUNTERPARTIES_PATH = PROJECT_ROOT / "data" / "counterparties.json"
OPERATIONS_PATH = PROJECT_ROOT / "data" / "operations.json"
SCHEMA_PATH = PROJECT_ROOT / "config" / "schema.json"


def load_json(path: Path) -> dict:
    """Загрузка JSON-файла."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_counterparties_text(data: dict) -> str:
    """Формирование текста справочника контрагентов для промпта."""
    lines = []
    for c in data.get("counterparties", []):
        notes = ", ".join(n for n in c.get("notes", []) if n)
        keywords = ", ".join(c.get("keywords", []))
        lines.append(f"- {c['name']}: примечания [{notes}], ключевые слова [{keywords}]")
    return "\n".join(lines)


def build_system_prompt() -> str:
    """Системный промпт с правилами и справочниками."""
    counterparties = load_json(COUNTERPARTIES_PATH)
    operations = load_json(OPERATIONS_PATH)
    schema = load_json(SCHEMA_PATH)

    cp_text = build_counterparties_text(counterparties)

    return f"""Ты парсер отчётов водителя мусоровоза. Анализируешь сообщения и извлекаешь структурированные данные.

## Справочник контрагентов (название + примечание):
{cp_text}

## Правила:
1. "козулева" в любом виде (козулева 2/4, 9к1) → примечание всегда "знак"
2. железно зарядное → Железно, Зарядное
3. железно знак / железно инноград → Железно-Киров, знак или инноград
4. управление домами + жуковского → Примечание Жуковского
5. управление домами + свободы → Примечание Свободы
6. Пустые сообщения и "Выехал/Закончил" (рабочее время) → пропускай (тип skip)
7. Нормализуй названия контрагентов строго по справочнику

## Типы операций:
- container_pickup: вывоз контейнеров (N контейнеров, контейнер + адрес)
- trip_removal: ходка — вывоз мусора с земли (не контейнером). Ключевые слова "ходка", "ходки" + название клиента. Примеры: "Ходка акмаш", "2 ходки маяк"
- landfill_unload: выгрузка на полигоне ("выгрузка в оричах", "N выгрузки")
- advance: аванс ("аванс взял N")
- skip: не обрабатывать (пустое, время выезда/окончания, вес и т.п.)

## Выход — JSON массив. Каждый элемент:
{{"type": "container_pickup"|"trip_removal"|"landfill_unload"|"advance", "date": "DD.MM.YYYY", "counterparty": "...", "note": "...", "object_count": N, "trip_count": N, "unload_count": N}}

Для container_pickup: date, counterparty, note, object_count (обязательно)
Для trip_removal: date, counterparty (по справочнику; акмаш→Акмаш, маяк→Маяк), note пустое, trip_count (кол-во ходок; если не указано — 1)
Для landfill_unload: date, unload_count (остальное пусто)
Для advance: date, counterparty="Водитель Зеленцов", note="аванс"
Для skip: не включай в массив

Если в сообщении нет даты — используй default_date из запроса.
Если несколько операций в одном сообщении — верни несколько элементов.
Отвечай ТОЛЬКО валидным JSON массивом, без markdown и пояснений."""


def parse_message(
    message_text: str,
    message_date: datetime,
    context_messages: list[str] | None = None,
) -> list[dict]:
    """
    Парсинг сообщения через OpenAI.

    Возвращает список записей для таблицы, каждая — dict с ключами:
    Дата, Месяц, Структура, КСП, Операция, КСЗ, Контрагент, Примечание, Объект
    """
    if not message_text or not message_text.strip():
        return []

    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    operations = load_json(OPERATIONS_PATH)
    schema = load_json(SCHEMA_PATH)
    default_date = message_date.strftime("%d.%m.%Y")

    context = ""
    if context_messages:
        context = "Предыдущие сообщения (для контекста):\n" + "\n".join(context_messages[-5:])

    user_content = f"""default_date: {default_date}

{context}

Сообщение для разбора:
{message_text.strip()}"""

    response = client.chat.completions.create(
        model=os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
        messages=[
            {"role": "system", "content": build_system_prompt()},
            {"role": "user", "content": user_content},
        ],
        temperature=0.1,
    )

    raw = response.choices[0].message.content.strip()
    # Убрать возможную обёртку в markdown
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1] if "\n" in raw else raw
        raw = raw.replace("```json", "").replace("```", "").strip()

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []

    if not isinstance(parsed, list):
        parsed = [parsed] if parsed else []

    rows = []
    for item in parsed:
        row = parse_item_to_row(item, operations, schema)
        if row:
            rows.append(row)

    return rows


def parse_item_to_row(item: dict, operations: dict, schema: dict) -> dict | None:
    """Преобразование элемента от OpenAI в строку для таблицы."""
    op_type = item.get("type")
    if op_type not in operations:
        return None

    op = operations[op_type]
    date_str = item.get("date", "")
    date_obj = None
    if date_str:
        try:
            date_obj = datetime.strptime(date_str, "%d.%m.%Y")
        except ValueError:
            pass

    if not date_obj:
        return None

    counterparty = item.get("counterparty", "")
    note = item.get("note", "")

    # Для выгрузки на полигоне — Объект пустой
    if op_type == "landfill_unload":
        object_count = ""
    elif op_type == "trip_removal":
        object_count = item.get("trip_count") or item.get("object_count") or "1"
    else:
        object_count = item.get("object_count") or ""

    # Нормализация для advance
    if op_type == "advance":
        counterparty = schema.get("advance_counterparty", "Водитель Зеленцов")
        note = "аванс"
        object_count = ""

    row = {
        "Дата": date_str,
        "Месяц": str(date_obj.month),
        "Структура": op["структура"],
        "КСП": op["ксп"],
        "Операция": op["операция"],
        "КСЗ": op["ксз"],
        "Контрагент": counterparty,
        "Примечание": note,
        "Объект": str(object_count) if object_count else "",
    }
    return row
