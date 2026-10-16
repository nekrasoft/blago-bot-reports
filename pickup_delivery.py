from __future__ import annotations

import fcntl
import logging
import os
from datetime import datetime
import threading

import httpx
from gspread.utils import ValueRenderOption

from persistent_context import state_directory
from map_client import _get_base_url, _get_write_api_headers
from sheets_client import _load_schema, _parse_sheet_url, _col_letter, get_sheets_client

logger = logging.getLogger(__name__)


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
    destination = max(1, len(worksheet.col_values(columns.index('Дата') + 1)))
    values = dict(row)
    values['Примечание'] = f"{row.get('Примечание', '')} {marker}".strip()
    cells = []
    for name in columns:
        value = values.get(name, '')
        if name == 'Дата':
            value = (datetime.strptime(str(value), '%d.%m.%Y') - datetime(1899, 12, 30)).days
        elif name in ('Объект', 'Расход', 'Приход') and value != '':
            value = float(str(value).replace(',', '.'))
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
    try:
        append_pickup_row(job['row'], job['submissionKey'])
        sent = True
    except Exception:
        logger.exception('Google Sheets: доставка отчёта %s будет повторена', job['id'])
    response = client.post(f'{base}/api.php', params={'route': 'pickup-delivery/ack'}, headers=_get_write_api_headers(),
        json={'id': job['id'], 'token': job['token'], 'sent': sent})
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
