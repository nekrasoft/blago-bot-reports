"""Driver expense validation and the corresponding cashbook row."""
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
import os
import re
from uuid import uuid4
from zoneinfo import ZoneInfo

STRUCTURES = {'1202': 'ЮЛ - Контейнеры', '1201': 'ЮЛ - Вывоз мусора'}
CATEGORIES = {'182': 'Ремонт а/м и ТО', '183': 'ГСМ', '185': 'Утилизация (полигон)',
              '117': 'Хозяйственные расходы', '115': 'Материалы', '124': 'Связь и интернет', '149': 'Штрафы ПДД'}
PRIMARY_CATEGORIES = ('182', '183', '185')


def expense_chat_allowed(chat_id) -> bool:
    allowed = {value.strip() for value in os.environ.get('ALLOWED_CHAT_IDS', '').split(',') if value.strip()}
    return not allowed or str(chat_id) in allowed


def parse_amount(text: str) -> str:
    text = text.strip()
    if not re.fullmatch(r'(?:\d{1,3}(?:[ \u00a0\u202f]\d{3})+|\d+)(?:[,.]\d{1,2})?', text):
        raise ValueError('Введите положительную сумму с точностью до копеек, например: 4000 или 4 000,50.')
    amount = Decimal(re.sub(r'\s', '', text).replace(',', '.'))
    if not Decimal('0') < amount <= Decimal('9999999999.99'):
        raise ValueError('Сумма должна быть больше нуля и не превышать 9 999 999 999,99 ₽.')
    return format(amount, '.2f').replace('.', ',')


def set_purpose(data: dict, text: str) -> None:
    text = text.strip()
    if not 1 <= len(text) <= 500:
        raise ValueError('Укажите, на что потратили деньги: от 1 до 500 символов.')
    data['expense_purpose'] = text


def confirmation_payload(data: dict) -> str:
    return 'mconfirm:' + data.setdefault('expense_confirmation_key', uuid4().hex)


def expense_summary(data: dict) -> str:
    return (f"Расход: {data['expense_amount']} ₽\n"
            f"Структура: {STRUCTURES[data['expense_structure']]}\n"
            f"Категория: {CATEGORIES[data['expense_category']]}\n"
            f"На что: {data['expense_purpose']}")


def prepare_expense(data: dict, *, source: str, user_id, name: str | None, now=None) -> dict:
    if data.get('expense_submission'):
        return data['expense_submission']
    if source not in ('telegram', 'max') or user_id is None:
        raise ValueError('Не удалось определить водителя.')
    structure = data['expense_structure']
    category = data['expense_category']
    if structure not in STRUCTURES or category not in CATEGORIES:
        raise ValueError('Выберите структуру и категорию расхода.')
    amount = parse_amount(data['expense_amount'])
    set_purpose(data, data.get('expense_purpose', ''))
    date = now or datetime.now(ZoneInfo('Europe/Moscow'))
    row = {'Дата': date.strftime('%d.%m.%Y'), 'Месяц': str(date.month),
           'Структура': STRUCTURES[structure], 'КСП': structure, 'Операция': CATEGORIES[category], 'КСЗ': category,
           'Контрагент': '', 'Примечание': data['expense_purpose'], 'Расход': amount, 'Приход': '',
           'Объект': '', 'Выручка': '', '_skip_formula_columns': ['Выручка'], '_formula_columns': ['Месяц', 'Сальдо']}
    submission = {'key': data.setdefault('expense_key', str(uuid4())), 'source': source,
                  'user_id': str(user_id), 'driver_name': name, 'row': row}
    data['expense_submission'] = deepcopy(submission)
    return data['expense_submission']
