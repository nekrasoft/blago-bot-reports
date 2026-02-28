# Клиент Google Sheets для записи данных

import json
import os
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

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
    :param sheet_url: URL таблицы или ID (из переменной GOOGLE_SHEET_ID)
    :param sheet_name: Имя листа (если пусто — первый лист)
    :return: Количество добавленных строк
    """
    if not rows:
        return 0

    client = get_sheets_client()
    sheet_id = sheet_url or os.environ.get("GOOGLE_SHEET_ID")
    if not sheet_id:
        raise ValueError("Укажите GOOGLE_SHEET_ID в .env или передайте sheet_url")

    spreadsheet = client.open_by_key(_extract_sheet_id(sheet_id))

    if sheet_name:
        worksheet = spreadsheet.worksheet(sheet_name)
    else:
        worksheet = spreadsheet.sheet1

    schema = _load_schema()
    all_columns = schema.get("google_sheet_columns", [])
    fill_columns = schema.get("fill_columns", [])

    for row_dict in rows:
        # Собираем полную строку по порядку колонок таблицы
        values = []
        for col in all_columns:
            if col in fill_columns:
                values.append(row_dict.get(col, ""))
            else:
                values.append("")
        worksheet.append_row(values, value_input_option="USER_ENTERED")

    return len(rows)


def _extract_sheet_id(url_or_id: str) -> str:
    """Извлечение ID таблицы из URL или возврат как есть, если уже ID."""
    if "/d/" in url_or_id:
        start = url_or_id.find("/d/") + 3
        end = url_or_id.find("/", start)
        return url_or_id[start:end] if end > 0 else url_or_id[start:]
    return url_or_id.strip()
