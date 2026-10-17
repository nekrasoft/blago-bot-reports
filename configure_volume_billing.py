"""Prepare volume billing columns and proposed tariffs. Read-only unless --apply."""
import argparse
from datetime import datetime
from decimal import Decimal, InvalidOperation
import json
import os
from pathlib import Path
import re

from dotenv import load_dotenv
from gspread.utils import ValueRenderOption

from map_client import get_trip_removal_counterparties
from sheets_client import get_sheets_client, _load_schema, _parse_sheet_url, _col_letter


def propose_tariff(price, formula=''):
    try:
        price = Decimal(str(price).replace('\u00a0', '').replace(' ', '').replace(',', '.'))
    except InvalidOperation:
        return None, 'Некорректная цена'
    if not price.is_finite() or price <= 0:
        return None, 'Нет положительной цены'
    explicit = re.search(r'\*\s*(30|36)\s*$', str(formula))
    bases = [int(explicit[1])] if explicit else [n for n in (30, 36) if price % n == 0]
    if not bases:
        return None, 'Не делится на 30 или 36 без остатка'
    base = bases[0]
    reason = f'Явная формула на {base} м³' if explicit else f'База {base} м³' + ('; делится также на 36' if len(bases) == 2 else '')
    return str(price / base), reason


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    load_dotenv(Path(__file__).with_name('.env'))
    schema = _load_schema()
    sid, gid = _parse_sheet_url(os.environ.get('GOOGLE_SHEET_URL') or schema['google_sheet_url'])
    book = get_sheets_client().open_by_key(sid)
    cash = book.get_worksheet_by_id(gid)
    prices = book.worksheet('Бункеры')
    cash_rows = cash.get_all_values(value_render_option=ValueRenderOption.formula)
    price_formulas = prices.get_all_values(value_render_option=ValueRenderOption.formula)
    price_rows = prices.get_all_values()
    additions = ['Объём, м³', 'Единица тарифа', 'Тариф на дату вывоза']
    base = schema['google_sheet_columns'][:16]
    if len(base) == 15:
        base = [*base, 'Документ']
    if len(cash_rows) < 2 or cash_rows[1][:16] != base:
        raise ValueError('Первые 16 колонок кассы отличаются от ожидаемых; автоматическое изменение остановлено.')
    if any(value for value in cash_rows[1][16:]) and cash_rows[1][16:19] != additions:
        raise ValueError('Новые колонки кассы заняты другими заголовками.')
    for row in cash_rows[2:]:
        if any(row[16:19]) and cash_rows[1][16:19] != additions:
            raise ValueError('В новых колонках кассы уже есть данные.')
    headers = list(price_rows[0])
    width = max(map(len, price_rows))
    headers += [''] * (width - len(headers))
    for name in ('Единица тарифа', 'Тариф за м³'):
        if name not in headers:
            headers.append(name)
    unit_col, tariff_col = headers.index('Единица тарифа'), headers.index('Тариф за м³')
    trip_names = {c['shortName'] or c['name'] for c in get_trip_removal_counterparties()}
    if not trip_names:
        raise ValueError('Справочник вывозов пуст; изменение остановлено.')
    updates = [{'range': 'Q2:S2', 'values': [additions]}]
    last_date_row = max(i for i, row in enumerate(cash_rows, 1) if len(row) > 2 and row[2])
    formula_updates = []
    for i in range(last_date_row + 1, len(cash_rows) + 1):
        row = cash_rows[i - 1]
        if len(row) > 14 and row[14]:
            continue
        lookup = f"VLOOKUP(H{i};'Бункеры'!$A:$F;{{column}};FALSE)"
        old_tariff = f'IF({lookup.format(column=6)}>0;{lookup.format(column=6)};{lookup.format(column=5)})'
        formula = f'=IF(OR(C{i}="";F{i}<>"Поступление по основной деятельности");"";IF(R{i}="м³";IF(OR(Q{i}="";S{i}="");NA();ROUND(Q{i}*S{i};2));M{i}*{old_tariff}))'
        formula_updates.append({'range': f'O{i}', 'values': [[formula]]})
    price_updates = [{'range': f'A1:{_col_letter(len(headers))}1', 'values': [headers]}]
    plan = []
    names = [str(row[0]).strip() for row in price_rows[1:] if row]
    for i, row in enumerate(price_rows[1:], 2):
        if not row or row[0] not in trip_names or row[0].casefold() == 'частник':
            continue
        if names.count(row[0]) != 1:
            raise ValueError(f'Дублирующийся контрагент: {row[0]}')
        existing = row[tariff_col] if len(row) > tariff_col else ''
        if existing:
            plan.append({'counterparty': row[0], 'tariff': existing, 'reason': 'Уже заполнено; не изменяется'})
            continue
        def cell(index):
            return row[index] if len(row) > index else ''
        price_col = 5 if cell(5).strip() and Decimal(cell(5).replace('\u00a0', '').replace(' ', '').replace(',', '.')) > 0 else 4
        formula_row = price_formulas[i - 1]
        tariff, reason = propose_tariff(cell(price_col), formula_row[price_col] if len(formula_row) > price_col else '')
        plan.append({'counterparty': row[0], 'old_price': cell(price_col), 'tariff': tariff, 'reason': reason})
        price_updates.append({'range': f'{_col_letter(unit_col+1)}{i}', 'values': [['м³']]})
        if tariff:
            price_updates.append({'range': f'{_col_letter(tariff_col+1)}{i}', 'values': [[float(tariff)]]})
    print(json.dumps({'apply': args.apply, 'tariffs': plan, 'missing_rows': sorted(trip_names - set(names) - {'Частник'})}, ensure_ascii=False, indent=2))
    if not args.apply:
        return
    directory = Path(__file__).parent / 'data/private-state/volume-billing-backups'
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    backup = directory / (datetime.now().strftime('%Y%m%d-%H%M%S-%f') + '.json')
    with backup.open('x') as output:
        os.chmod(backup, 0o600)
        json.dump({'cash': cash_rows, 'prices': price_formulas, 'plan': plan}, output, ensure_ascii=False)
    if cash.col_count < 19:
        cash.add_cols(19 - cash.col_count)
    if prices.col_count < len(headers):
        prices.add_cols(len(headers) - prices.col_count)
    prices.batch_update(price_updates, value_input_option='RAW')
    cash.batch_update(updates, value_input_option='RAW')
    if formula_updates:
        cash.batch_update(formula_updates, value_input_option='USER_ENTERED')
    print('Новые колонки и предварительные тарифы сохранены. Старые формулы и цены не изменялись.')


if __name__ == '__main__':
    main()
