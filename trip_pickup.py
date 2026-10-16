"""Shared trip report data for Telegram and MAX; delivery uses the pickup outbox."""
import json
import re
from datetime import datetime
from uuid import uuid4
from pathlib import Path
from decimal import Decimal, InvalidOperation
from hashlib import sha256

from bunker_pickup import MAX_SITE_PHOTOS
from map_client import get_all_bunkers
from pickup_submission import prepare_submission
from waybill_notes import format_note_with_waybill_token


OPERATIONS_PATH = Path(__file__).resolve().parent / "data" / "operations.json"


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


def _parse_volume(text: str) -> Decimal | None:
    match = re.search(r"\d+(?:[,.]\d+)?", text or "")
    if not match:
        return None
    try:
        value = Decimal(match.group(0).replace(",", "."))
    except InvalidOperation:
        return None
    return value if value > 0 else None


def _is_private_contractor(contractor: str) -> bool:
    return contractor.strip().casefold() == "частник"


def trip_sites(counterparty_id) -> list[dict]:
    sites = {}
    for bunker in get_all_bunkers(counterparty_id=counterparty_id):
        if str(bunker.get('counterpartyId')) != str(counterparty_id):
            continue
        address = str(bunker.get('address') or '').strip()
        district = str(bunker.get('district') or '').strip()
        if address:
            sites[(district, address)] = {'district': district, 'address': address}
    return list(sites.values())


def manual_site(text: str) -> dict:
    parts = text.split(';', 1)
    if len(parts) != 2 or not all(part.strip() for part in parts):
        raise ValueError('Введите район и адрес через «;», например: Знак; ул. Дмитрия Козулева, 10к1')
    district, address = [part.strip() for part in parts]
    if len(district) > 255 or len(address) > 500:
        raise ValueError('Район — до 255 символов, адрес — до 500.')
    return {'district': district, 'address': address}


def add_trip_photos(data: dict, files: list[dict]) -> int:
    photos = data.setdefault('hodka_photos', [])
    def identity(file):
        return file.get('source_file_id') or sha256(file.get('file_bytes') or b'').hexdigest()
    seen = {identity(photo) for photo in photos}
    incoming = []
    if not files:
        raise ValueError('Прикрепите изображение кузова или нажмите «Готово / Без фото».')
    for file in files:
        key = identity(file)
        if key not in seen:
            incoming.append(file)
            seen.add(key)
    if len(photos) + len(incoming) > MAX_SITE_PHOTOS:
        raise ValueError(f'Можно прикрепить не более {MAX_SITE_PHOTOS} фото. Уже загружено: {len(photos)}.')
    if any(not str(f.get('content_type', '')).startswith('image/') for f in incoming):
        raise ValueError('Для фото кузова принимаются только изображения.')
    photos.extend(incoming)
    return len(photos)


def confirmation_payload(data: dict) -> str:
    key = data.setdefault('hodka_confirmation_key', uuid4().hex)
    return f'hconfirm:{key}'


def trip_summary(data: dict) -> str:
    site = data['hodka_site']
    text = (f"Ходки: {data['hodka_selected_contractor']}\n"
            f"Площадка: {site['district']}, {site['address']}\n"
            f"Количество ходок: {data['hodka_trips_count']}\n{data['hodka_volume_note']}\n"
            f"Фото: {len(data.get('hodka_photos') or [])}\n"
            f"Путевой лист: {'приложен' if data.get('hodka_waybill') else 'не приложен'}")
    if data.get('hodka_cash_income'):
        text += f"\nНаличные: {data['hodka_cash_income']} ₽"
    return text


def prepare_trip_submission(data: dict, *, source, user_id, name, chat_id, message_id=None):
    if data.get('hodka_submission'):
        return data['hodka_submission']
    volume = _parse_volume(data['hodka_volume_note'])
    if volume is None or volume > 100000 or not 1 <= int(data['hodka_trips_count']) <= 1000:
        raise ValueError('Проверьте количество ходок и общий объём.')
    if not data.get('hodka_counterparty_id'):
        raise ValueError('Не выбран контрагент. Запустите /h заново.')
    site = data['hodka_site']
    note = f"{data['hodka_volume_note']}, {site['district']}, {site['address']}"
    row = _build_trip_row(data['hodka_selected_contractor'], data['hodka_trips_count'], data['hodka_date_str'], note,
                          cash_income=data.get('hodka_cash_income') or None)
    draft = {'site_photos': data.get('hodka_photos') or [],
             'waybills': [data['hodka_waybill']] if data.get('hodka_waybill') else []}
    submission = prepare_submission(draft, source=source, user_id=user_id, name=name, chat_id=chat_id, message_id=message_id,
        trip_details={'counterpartyId': data['hodka_counterparty_id'], 'tripsCount': data['hodka_trips_count'],
                      'tripVolumeM3': str(volume), 'district': site['district'], 'address': site['address'],
                      'cashIncome': str(Decimal(data['hodka_cash_income'].replace(',', '.'))) if data.get('hodka_cash_income') else None},
        sheet_row=row)
    for token in submission['request']['waybill_tokens'] + submission['photo_tokens']:
        row['Примечание'] = format_note_with_waybill_token(row['Примечание'], token)
    # Freeze the exact same row and tokens for retries, including an ambiguous API response.
    submission['row'] = row
    data['hodka_submission'] = submission
    return submission
