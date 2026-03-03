# Клиент Google Sheets для записи данных
from __future__ import annotations

import json
import os
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials
from gspread.utils import ValueRenderOption

PROJECT_ROOT = Path(__file__).resolve().parent
CREDENTIALS_PATH = PROJECT_ROOT / "credentials" / "google_service_account.json"
SCHEMA_PATH = PROJECT_ROOT / "config" / "schema.json"

# Области доступа для Google API
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


def _load_schema() -> dict:
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def get_sheets_client() -> gspread.Client:
    """Создание клиента gspread с учётными данными сервисного аккаунта."""
    creds_path = os.environ.get("GOOGLE_CREDENTIALS_PATH", str(CREDENTIALS_PATH))
    credentials = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    return gspread.authorize(credentials)


def append_rows(rows: list[dict], sheet_url: str | None = None, sheet_name: str | None = None) -> int:
    """
    Добавление строк в Google-таблицу.

    :param rows: Список словарей с ключами Дата, Месяц, Структура, КСП, Операция, КСЗ, Контрагент, Примечание, Объект
    :param sheet_url: URL таблицы (переопределяет config)
    :param sheet_name: Имя листа (переопределяет gid из URL)
    :return: Количество добавленных строк
    """
    if not rows:
        return 0

    client = get_sheets_client()
    schema = _load_schema()

    url = sheet_url or os.environ.get("GOOGLE_SHEET_URL") or schema.get("google_sheet_url")
    if not url:
        raise ValueError("Укажите google_sheet_url в config/schema.json или GOOGLE_SHEET_URL в .env")

    sheet_id, gid = _parse_sheet_url(url)
    spreadsheet = client.open_by_key(sheet_id)

    if sheet_name:
        worksheet = spreadsheet.worksheet(sheet_name)
    elif gid is not None:
        worksheet = spreadsheet.get_worksheet_by_id(gid)
    else:
        worksheet = spreadsheet.sheet1

    all_columns = schema.get("google_sheet_columns", [])
    fill_columns = schema.get("fill_columns", [])

    # Находим последнюю строку с данными по колонке "Дата" (всегда заполнена нами)
    col_data_idx = all_columns.index("Дата") + 1 if "Дата" in all_columns else 1
    last_row = len(worksheet.col_values(col_data_idx))
    next_row = last_row + 1

    # Колонки с формулами: B (Месяц) и O (Выручка) — копируем формулу с предыдущей строки
    formula_column_names = schema.get("formula_columns", ["Месяц", "Выручка"])
    formula_col_indices = [
        all_columns.index(name) for name in formula_column_names if name in all_columns
    ]

    # Получаем формулы из последней строки с данными
    formulas_by_col = {}
    if last_row >= 1 and formula_col_indices:
        for col_idx in formula_col_indices:
            cell = worksheet.cell(
                last_row, col_idx + 1, value_render_option=ValueRenderOption.formula
            )
            if cell.value and str(cell.value).startswith("="):
                formulas_by_col[col_idx] = str(cell.value)

    # Обновляем только fill_columns и formula_columns, остальные не трогаем
    update_col_indices = sorted(set(
        formula_col_indices + [all_columns.index(c) for c in fill_columns if c in all_columns]
    ))

    for row_dict in rows:
        values_by_idx = {}
        for col_idx in update_col_indices:
            col = all_columns[col_idx]
            if col_idx in formulas_by_col:
                formula = formulas_by_col[col_idx]
                formula = formula.replace(str(last_row), str(next_row))
                values_by_idx[col_idx] = formula
            elif col in fill_columns:
                values_by_idx[col_idx] = row_dict.get(col, "")

        # Группируем в непрерывные диапазоны для минимума вызовов API
        ranges_to_update = _get_contiguous_ranges(update_col_indices)

        for start_idx, end_idx in ranges_to_update:
            values_part = [values_by_idx.get(i, "") for i in range(start_idx, end_idx + 1)]
            range_a1 = f"{_col_letter(start_idx + 1)}{next_row}:{_col_letter(end_idx + 1)}{next_row}"
            worksheet.update(range_a1, [values_part], value_input_option="USER_ENTERED")

        for col_idx in formulas_by_col:
            formula_in_values = values_by_idx.get(col_idx)
            if isinstance(formula_in_values, str) and formula_in_values.startswith("="):
                formulas_by_col[col_idx] = formula_in_values
        last_row = next_row
        next_row += 1

    return len(rows)


def _get_contiguous_ranges(indices: list[int]) -> list[tuple[int, int]]:
    """Разбивает отсортированный список индексов на непрерывные диапазоны (start, end)."""
    if not indices:
        return []
    ranges = []
    start = indices[0]
    prev = indices[0]
    for i in indices[1:]:
        if i == prev + 1:
            prev = i
        else:
            ranges.append((start, prev))
            start = prev = i
    ranges.append((start, prev))
    return ranges


def _col_letter(n: int) -> str:
    """Преобразование номера колонки (1-based) в букву A1-нотации."""
    result = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result


def _parse_sheet_url(url: str) -> tuple[str, int | None]:
    """
    Извлечение ID таблицы и gid листа из URL.

    Поддерживает gid в query (?gid=679928865) и во фрагменте (#gid=679928865).

    :return: (spreadsheet_id, gid или None если не указан)
    """
    from urllib.parse import urlparse, parse_qs

    sheet_id = ""
    gid = None

    if "/d/" in url:
        start = url.find("/d/") + 3
        rest = url[start:]
        sheet_id = rest.split("/")[0].split("?")[0]
    else:
        sheet_id = url.strip()

    parsed = urlparse(url)
    for part in (parsed.query, parsed.fragment):
        if part and "gid=" in part:
            params = parse_qs(part)
            if "gid" in params:
                gid = int(params["gid"][0])
                break

    return sheet_id, gid
