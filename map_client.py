# Клиент API карты бункеров — обновление даты вывоза

from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path

try:
    import httpx
except ImportError:
    httpx = None

PROJECT_ROOT = Path(__file__).resolve().parent

logger = logging.getLogger(__name__)

# Маппинг Примечание → district в карте
NOTE_TO_DISTRICT = {
    "зарядное": "Зарядное",
    "знак": "Знак",
    "инноград": "Инноград",
}


def _get_base_url() -> str | None:
    """Базовый URL API карты из .env."""
    url = os.environ.get("MAP_SERVICE_URL", "").rstrip("/")
    return url if url else None


def _get_api_headers() -> dict:
    """Заголовки для запросов к API карты (API-ключ бота при наличии)."""
    key = os.environ.get("MAP_BOT_API_KEY", "").strip()
    if key:
        return {"X-API-Key": key}
    return {}


def get_all_bunkers() -> list[dict]:
    """Получение всех бункеров с карты."""
    base = _get_base_url()
    if not base or not httpx:
        return []
    try:
        resp = httpx.get(f"{base}/api/bunkers", timeout=10.0)
        resp.raise_for_status()
        data = resp.json()
        logger.info("Карта: GET /api/bunkers — загружено %s бункеров", len(data))
        return data
    except httpx.HTTPStatusError as e:
        logger.warning("Карта: GET /api/bunkers — HTTP %s, %s", e.response.status_code, (e.response.text or "")[:150])
        return []
    except Exception as e:
        logger.warning("Карта: GET /api/bunkers — ошибка: %s", e)
        return []


def get_bunkers(contractor: str, district: str | None = None) -> list[dict]:
    """Получение списка бункеров по контрагенту и опционально району."""
    base = _get_base_url()
    if not base or not httpx:
        return []

    params = {"contractor": contractor}
    if district:
        params["district"] = district

    try:
        resp = httpx.get(f"{base}/api/bunkers", params=params, timeout=10.0)
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as e:
        logger.warning("Карта: GET /api/bunkers?contractor=... — HTTP %s", e.response.status_code)
        return []
    except Exception as e:
        logger.warning("Карта: GET /api/bunkers — ошибка: %s", e)
        return []


# fillLevel 0 = зелёный цвет (пусто после вывоза)
FILL_LEVEL_AFTER_PICKUP = 0
# fillLevel 100 = красный (заявка на опустошение)
FILL_LEVEL_REQUEST = 100


def set_bunker_fill_level(bunker_id: str, fill_level: int) -> bool:
    """Установка заполненности бункера (0–100). Для заявок — 100%."""
    base = _get_base_url()
    if not base:
        logger.warning("MAP_SERVICE_URL не задан — обновление карты недоступно")
        return False
    if not httpx:
        logger.warning("httpx не установлен — обновление карты недоступно")
        return False

    url = f"{base}/api/bunkers/{bunker_id}"
    headers = _get_api_headers()
    try:
        resp = httpx.put(
            url,
            json={"fillLevel": max(0, min(100, fill_level))},
            headers=headers,
            timeout=10.0,
        )
        resp.raise_for_status()
        logger.info("Карта: PUT /api/bunkers/%s — fillLevel=%s, успешно", bunker_id, fill_level)
        return True
    except httpx.HTTPStatusError as e:
        logger.warning(
            "Карта: PUT /api/bunkers/%s — HTTP %s, %s",
            bunker_id, e.response.status_code, (e.response.text or "")[:150],
        )
        return False
    except Exception as e:
        logger.warning("Карта: PUT /api/bunkers/%s — ошибка: %s", bunker_id, e)
        return False


def update_bunker_pickup_date(bunker_id: str, date_str: str) -> bool:
    """Обновление даты вывоза и заполненности бункера (зелёный после вывоза)."""
    base = _get_base_url()
    if not base or not httpx:
        return False

    headers = _get_api_headers()
    try:
        resp = httpx.put(
            f"{base}/api/bunkers/{bunker_id}",
            json={
                "lastPickupDate": date_str,
                "fillLevel": FILL_LEVEL_AFTER_PICKUP,
            },
            headers=headers,
            timeout=10.0,
        )
        resp.raise_for_status()
        logger.info("Карта: PUT /api/bunkers/%s — lastPickupDate=%s, fillLevel=0, успешно", bunker_id, date_str)
        return True
    except httpx.HTTPStatusError as e:
        logger.warning("Карта: PUT /api/bunkers/%s — HTTP %s, %s", bunker_id, e.response.status_code, (e.response.text or "")[:150])
        return False
    except Exception as e:
        logger.warning("Карта: PUT /api/bunkers/%s — ошибка: %s", bunker_id, e)
        return False


def _date_to_iso(date_str: str) -> str:
    """Преобразование DD.MM.YYYY в YYYY-MM-DD."""
    try:
        dt = datetime.strptime(date_str, "%d.%m.%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return date_str


def _is_container_pickup(row: dict) -> bool:
    """Проверка: строка — вывоз контейнеров (не ходка, не выгрузка)."""
    return (
        row.get("Структура") == "ЮЛ - Контейнеры"
        and row.get("Операция") == "Поступление по основной деятельности"
    )


def _get_district_for_note(note: str) -> str | None:
    """Получение district по примечанию."""
    n = (note or "").strip().lower()
    return NOTE_TO_DISTRICT.get(n)


def _filter_bunkers_by_note(bunkers: list[dict], note: str) -> list[dict]:
    """Дополнительная фильтрация бункеров по примечанию (адрес/район)."""
    if not note or not bunkers:
        return bunkers

    district = _get_district_for_note(note)
    if district:
        filtered = [b for b in bunkers if (b.get("district") or "") == district]
        return filtered if filtered else bunkers

    # Ищем по вхождению в адрес (улица, число)
    note_lower = note.lower().strip()
    note_clean = note_lower.replace(" ", "").replace(".", "")
    result = []
    for b in bunkers:
        addr = (b.get("address") or "").lower()
        addr_clean = addr.replace(" ", "").replace(".", "").replace(",", "")
        if note_lower in addr or note_clean in addr_clean:
            result.append(b)
        else:
            # Частичное совпадение: "хлебозаводская" ~ "хлебозаводской"
            for word in note_lower.split():
                if len(word) >= 4 and word[:6] in addr_clean:
                    result.append(b)
                    break
    return result if result else bunkers


def update_map_pickup_dates(rows: list[dict]) -> int:
    """
    Обновление lastPickupDate в карте для строк-вывозов контейнеров.

    Сопоставление: Контрагент → contractor, Примечание → district/address.
    Обновляются бункера с наиболее старой датой вывоза (до object_count шт).

    :return: Количество обновлённых бункеров
    """
    if not _get_base_url():
        return 0
    if not httpx:
        logger.warning("httpx не установлен — обновление карты недоступно")
        return 0

    updated = 0
    for row in rows:
        if not _is_container_pickup(row):
            continue

        counterparty = row.get("Контрагент", "").strip()
        note = row.get("Примечание", "").strip()
        date_str = row.get("Дата", "")
        object_count_raw = row.get("Объект", "1")
        try:
            object_count = max(1, int(object_count_raw)) if object_count_raw else 1
        except (ValueError, TypeError):
            object_count = 1

        if not counterparty or not date_str:
            continue

        iso_date = _date_to_iso(date_str)
        bunkers = get_bunkers(counterparty)
        bunkers = _filter_bunkers_by_note(bunkers, note) if note else bunkers

        # Сортируем по lastPickupDate (старые первыми), обновляем до object_count шт
        bunkers.sort(key=lambda b: b.get("lastPickupDate", ""))

        for b in bunkers[:object_count]:
            if update_bunker_pickup_date(b["id"], iso_date):
                updated += 1

    if updated:
        logger.info("Карта: обновлено %s бункеров (дата вывоза)", updated)
    return updated


def record_pickup_by_bunker_id(
    bunker_id: str,
    date_str: str,
    object_count: int = 1,
    bunkers_cache: list[dict] | None = None,
) -> tuple[dict | None, bool]:
    """
    Запись вывоза по id бункера: данные для таблицы + обновление карты.

    :return: (row для append_rows, успех обновления карты)
    """
    bunkers = bunkers_cache if bunkers_cache is not None else get_all_bunkers()
    bunker = next((b for b in bunkers if b.get("id") == bunker_id), None)
    if not bunker:
        return None, False

    contractor = bunker.get("contractor", "")
    address = bunker.get("address", "")
    district = bunker.get("district", "")
    note = district if district else _address_to_note(address)

    try:
        dt = datetime.strptime(date_str, "%d.%m.%Y")
    except ValueError:
        dt = datetime.now()

    row = {
        "Дата": date_str,
        "Месяц": str(dt.month),
        "Структура": "ЮЛ - Контейнеры",
        "КСП": "1202",
        "Операция": "Поступление по основной деятельности",
        "КСЗ": "1001",
        "Контрагент": contractor,
        "Примечание": note,
        "Объект": str(object_count),
    }

    iso_date = _date_to_iso(date_str)
    map_ok = update_bunker_pickup_date(bunker_id, iso_date)
    return row, map_ok


def build_container_pickup_row(
    contractor: str,
    note: str,
    object_count: int,
    date_str: str,
) -> dict:
    """Строка для таблицы: вывоз контейнеров (для групповой записи)."""
    try:
        dt = datetime.strptime(date_str, "%d.%m.%Y")
    except ValueError:
        dt = datetime.now()
    return {
        "Дата": date_str,
        "Месяц": str(dt.month),
        "Структура": "ЮЛ - Контейнеры",
        "КСП": "1202",
        "Операция": "Поступление по основной деятельности",
        "КСЗ": "1001",
        "Контрагент": contractor,
        "Примечание": note,
        "Объект": str(object_count),
    }


def _address_to_note(addr: str) -> str:
    """Краткое примечание из адреса (без «Киров, »)."""
    if not addr:
        return ""
    if "," in addr:
        return addr.split(",", 1)[1].strip()
    return addr


def get_bunker_log_entry(bunker_id: str) -> dict | None:
    """Контрагент, примечание, номер и адрес для лога. None если бункер не найден."""
    bunkers = get_all_bunkers()
    bunker = next((b for b in bunkers if b.get("id") == bunker_id), None)
    if not bunker:
        return None
    contractor = bunker.get("contractor", "")
    address = bunker.get("address", "")
    district = bunker.get("district", "")
    note = district if district else _address_to_note(address)
    number = bunker.get("number", "?")
    return {"contractor": contractor, "note": note, "number": number, "address": address}
