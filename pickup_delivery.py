from __future__ import annotations

import fcntl
import logging
import os
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from datetime import datetime
import threading

import httpx
from gspread.utils import ValueRenderOption

from persistent_context import state_directory
from map_client import _get_base_url, _get_write_api_headers
from sheets_client import _load_schema, _parse_sheet_url, _col_letter, get_sheets_client

logger = logging.getLogger(__name__)


def apply_volume_tariff(spreadsheet, row: dict) -> dict:
    """Resolve the tariff only for new volume-based rows; the sheet row is the snapshot."""
    if row.get('Единица тарифа') != 'м³':
        return row
    def positive(value):
        try:
            number = Decimal(str(value).replace('\u00a0', '').replace('\u202f', '').replace(' ', '').replace('₽', '').replace(',', '.'))
        except InvalidOperation:
            raise ValueError('Не заполнен корректный объём или тариф за м³.')
        if not number.is_finite() or number <= 0:
            raise ValueError('Объём и тариф за м³ должны быть больше нуля.')
        return number
    volume = positive(row.get('Объём, м³', ''))
    values = spreadsheet.worksheet('Бункеры').get_all_values()
    aliases = {'Ед.': 'Единица тарифа', 'Тариф': 'Тариф за м³'}
    headers = [aliases.get(str(name).strip(), str(name).strip()) for name in values[0]] if values else []
    if not {'Контрагент', 'Единица тарифа', 'Тариф за м³'}.issubset(headers):
        raise ValueError('На вкладке «Бункеры» нужны колонки «Контрагент», «Ед.» и «Тариф».')
    if any(headers.count(name) != 1 for name in ('Контрагент', 'Единица тарифа', 'Тариф за м³')):
        raise ValueError('На вкладке «Бункеры» повторяются колонки контрагента или тарифа.')
    prices = [dict(zip(headers, values_row)) for values_row in values[1:]]
    matches = [p for p in prices if str(p.get('Контрагент', '')).strip() == row['Контрагент']]
    if len(matches) != 1 or matches[0].get('Единица тарифа') != 'м³':
        raise ValueError(f"Заполните тариф за м³ для {row['Контрагент']} на вкладке «Бункеры».")
    tariff = positive(matches[0].get('Тариф за м³', '')).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    if tariff <= 0:
        raise ValueError('Тариф за м³ после округления должен быть больше нуля.')
    return {**row, 'Тариф на дату вывоза': str(tariff),
            'Выручка': str((volume * tariff).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)),
            '_skip_formula_columns': [*row.get('_skip_formula_columns', []), 'Выручка']}


def append_pickup_row(row: dict, submission_key: str, *, marker_kind: str = 'ВЫВОЗ') -> None:
    """Write one atomic row; recover an ambiguous result by its marker."""
    client = get_sheets_client()
    client.set_timeout(60)
    schema = _load_schema()
    url = os.environ.get('GOOGLE_SHEET_URL') or schema.get('google_sheet_url')
    if not url:
        raise ValueError('Не задана расчётная таблица')
    sheet_id, gid = _parse_sheet_url(url)
    spreadsheet = client.open_by_key(sheet_id)
    worksheet = spreadsheet.get_worksheet_by_id(gid) if gid is not None else spreadsheet.sheet1
    columns = schema['google_sheet_columns']
    marker = f'[{marker_kind}:{submission_key}]'
    notes = worksheet.col_values(columns.index('Примечание') + 1)
    matches = [index + 1 for index, note in enumerate(notes) if marker in str(note)]
    if len(matches) > 1:
        raise ValueError('В таблице обнаружены повторные метки отчёта')
    if matches:
        return
    if row.get('Единица тарифа') == 'м³':
        headers = worksheet.row_values(2)
        if headers[:len(columns)] != columns:
            raise ValueError('Обновите заголовки кассы перед записью объёмного тарифа.')
        row = apply_volume_tariff(spreadsheet, row)
    destination = max(1, len(worksheet.col_values(columns.index('Дата') + 1)))
    values = dict(row)
    values['Примечание'] = f"{row.get('Примечание', '')} {marker}".strip()
    cells = []
    for name in columns:
        value = values.get(name, '')
        if name == 'Дата':
            value = (datetime.strptime(str(value), '%d.%m.%Y') - datetime(1899, 12, 30)).days
        elif name in ('Объект', 'Расход', 'Приход', 'Объём, м³', 'Тариф на дату вывоза', 'Выручка') and value != '':
            value = float(str(value).replace(',', '.'))
        if name == 'Выручка' and row.get('Единица тарифа') == 'м³':
            volume_cell = _col_letter(columns.index('Объём, м³') + 1) + str(destination + 1)
            tariff_cell = _col_letter(columns.index('Тариф на дату вывоза') + 1) + str(destination + 1)
            row_number = destination + 1
            unit_cell = _col_letter(columns.index('Единица тарифа') + 1) + str(row_number)
            count_cell = _col_letter(columns.index('Объект') + 1) + str(row_number)
            customer_cell = _col_letter(columns.index('Контрагент') + 1) + str(row_number)
            lookup = f"VLOOKUP({customer_cell};'Бункеры'!$A:$F;{{column}};FALSE)"
            old_tariff = f'IF({lookup.format(column=6)}>0;{lookup.format(column=6)};{lookup.format(column=5)})'
            formula = f'=IF({unit_cell}="м³";ROUND({volume_cell}*{tariff_cell};2);{count_cell}*{old_tariff})'
            cells.append({'userEnteredValue': {'formulaValue': formula}})
            continue
        cells.append({'userEnteredValue': {'numberValue': value} if isinstance(value, (int, float)) else {'stringValue': str(value)}})
    area = dict(sheetId=worksheet.id, startRowIndex=destination, endRowIndex=destination + 1)
    requests = [
        {'insertDimension': {'range': dict(sheetId=worksheet.id, dimension='ROWS', startIndex=destination, endIndex=destination + 1),
                             'inheritFromBefore': True}},
        {'repeatCell': {'range': dict(area, startColumnIndex=columns.index('Дата'), endColumnIndex=columns.index('Дата') + 1),
                        'cell': {'userEnteredFormat': {'numberFormat': {'type': 'DATE', 'pattern': 'dd.mm.yyyy'}}},
                        'fields': 'userEnteredFormat.numberFormat'}},
    ]
    # Expenses must leave the sheet-managed balance untouched, including queued older payloads.
    balance = columns.index('Сальдо') if marker_kind == 'РАСХОД' and 'Сальдо' in columns else len(columns)
    for start, end in ((0, balance), (balance + 1, len(columns))):
        if start < end:
            requests.append({'updateCells': {'start': dict(sheetId=worksheet.id, rowIndex=destination, columnIndex=start),
                                             'rows': [{'values': cells[start:end]}], 'fields': 'userEnteredValue'}})
    for name in row.get('_formula_columns', schema.get('formula_columns', ['Месяц', 'Выручка'])):
        if name not in columns or name in row.get('_skip_formula_columns', []) or (marker_kind == 'РАСХОД' and name == 'Сальдо'):
            continue
        column = columns.index(name)
        letter = _col_letter(column + 1)
        formulas = worksheet.get(f'{letter}1:{letter}{destination}', value_render_option=ValueRenderOption.formula)
        for index in range(len(formulas) - 1, -1, -1):
            if formulas[index] and str(formulas[index][0]).startswith('='):
                source = dict(sheetId=worksheet.id, startRowIndex=index, endRowIndex=index + 1,
                              startColumnIndex=column, endColumnIndex=column + 1)
                requests.append({'copyPaste': {'source': source, 'destination': dict(area, startColumnIndex=column, endColumnIndex=column + 1),
                                               'pasteType': 'PASTE_FORMULA'}})
                break
    # Inserting the row, values, marker and formulas is one atomic Sheets operation.
    spreadsheet.batch_update({'requests': requests})


def deliver_one(client: httpx.Client) -> bool:
    base = _get_base_url()
    if not base:
        return False
    response = client.post(f'{base}/api.php', params={'route': 'pickup-delivery/claim'}, headers=_get_write_api_headers())
    response.raise_for_status()
    job = response.json().get('job')
    if job is None:
        return False
    sent = False
    error = None
    try:
        append_pickup_row(job['row'], job['submissionKey'])
        sent = True
    except Exception as exc:
        if isinstance(exc, ValueError):
            error = str(exc)[:255]
        logger.exception('Google Sheets: доставка отчёта %s будет повторена', job['id'])
    response = client.post(f'{base}/api.php', params={'route': 'pickup-delivery/ack'}, headers=_get_write_api_headers(),
        json={'id': job['id'], 'token': job['token'], 'sent': sent, 'error': error})
    response.raise_for_status()
    return True


def start_delivery_worker() -> None:
    def run():
        from expense_outbox import deliver_expense_one
        with (state_directory() / 'pickup-delivery.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            with httpx.Client(timeout=60) as client:
                while True:
                    try:
                        worked = deliver_one(client)
                        delay = 2 if worked else 15
                    except Exception:
                        logger.exception('Не удалось обработать очередь расчётной таблицы')
                        delay = 60
                    try:
                        if deliver_expense_one():
                            delay = 2
                    except Exception:
                        logger.exception('Не удалось доставить расход в кассу; попытка будет повторена')
                    threading.Event().wait(delay)
    threading.Thread(target=run, name='pickup-sheet-delivery', daemon=True).start()
