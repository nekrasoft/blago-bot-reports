from __future__ import annotations

from decimal import Decimal

from map_client import build_container_pickup_row, format_note_with_bunker_numbers
from waybill_notes import format_note_with_waybill_token

ALLOWED_BILLING_UNITS = (Decimal("1"), Decimal("1.5"), Decimal("2"))
MAX_SITE_PHOTOS = 10
MAX_WAYBILLS = 5


def pickup_group_key(item: dict) -> tuple[str, str]:
    counterparty = str(
        item.get("counterpartyId")
        or item.get("counterparty_id")
        or item.get("contractor")
        or ""
    ).strip().casefold()
    location = str(
        item.get("district")
        or item.get("District")
        or item.get("address")
        or item.get("note")
        or ""
    ).strip().casefold()
    return counterparty, location


def parse_billing_units(value: object) -> Decimal | None:
    try:
        units = Decimal(str(value).replace(",", "."))
    except Exception:
        return None
    return units if units in ALLOWED_BILLING_UNITS else None


def format_billing_units(value: object) -> str:
    units = Decimal(str(value))
    return format(units.normalize(), "f").replace(".", ",")


def total_billing_units(items: list[dict]) -> Decimal:
    return sum(
        (Decimal(str(item.get("billing_units") or 1)) for item in items),
        Decimal("0"),
    )


def build_pickup_sheet_row(items: list[dict], date_str: str, waybill_tokens: list[str] | None) -> dict:
    if not items:
        raise ValueError("Нет бункеров для отчёта")
    first = items[0]
    note = format_note_with_bunker_numbers(
        str(first.get("note") or ""),
        [item.get("number") for item in items],
    )
    for token in waybill_tokens or []:
        note = format_note_with_waybill_token(note, token)
    return build_container_pickup_row(
        str(first.get("contractor") or ""),
        note,
        format_billing_units(total_billing_units(items)),
        date_str,
    )


def pickup_confirmation(items: list[dict], cleanup_status: str, cleanup_comment: str | None,
                        photos: list[dict], waybills: list[dict], missing_reason: str | None) -> str:
    first = items[0]
    lines = [f"Контрагент: {first.get('contractor', '')}",
             f"Площадка: {first.get('address') or first.get('note') or '—'}"]
    lines.extend(f"• №{item.get('number', '?')} × {format_billing_units(item['billing_units'])}" for item in items)
    volume = sum((Decimal(str(item.get('volume', 8))) * Decimal(str(item['billing_units']))
                  for item in items), Decimal('0'))
    cleanup = {'cleaned': 'Прибрана', 'not_required': 'Не требовалась', 'not_cleaned': 'Не прибрана'}
    lines.extend([f"Вывезено: {len(items)} бункеров",
                  f"К оплате: {format_billing_units(total_billing_units(items))} единиц",
                  f"Расчётный объём: {format_billing_units(volume)} м³",
                  f"Уборка: {cleanup.get(cleanup_status, 'Не указана')}"])
    if cleanup_comment:
        lines.append(f"Комментарий: {cleanup_comment}")
    lines.extend([f"Фото: {len(photos)}", f"Талоны: {len(waybills)}"])
    if not waybills and first.get('requiresContainerWaybill'):
        lines.append(f"⚠️ Нет обязательного талона: {missing_reason or 'причина не указана'}")
    return "\n".join(lines)
