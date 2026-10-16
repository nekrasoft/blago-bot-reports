import json
import sqlite3
import tempfile
import unittest
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from expense import parse_amount, prepare_expense, confirmation_payload, expense_chat_allowed
import expense_outbox
import expense_report
from expense_max import register_expense_handlers
from pickup_delivery import append_pickup_row
from test_pickup_delivery import Sheet


def expense_data():
    return {'expense_category': '182', 'expense_amount': '4000', 'expense_purpose': 'б/у колесо'}


class ExpenseTest(unittest.TestCase):
    def test_row_matches_cashbook_example_and_stays_frozen_on_retry(self):
        data = expense_data()
        first = prepare_expense(data, source='telegram', user_id=42, name='Водитель', now=datetime(2026, 6, 23))
        row = first['row']
        self.assertEqual([row[k] for k in ['Дата','Месяц','Структура','КСП','Операция','КСЗ','Примечание','Расход']],
                         ['23.06.2026','6','ЮЛ - Контейнеры','1202','Ремонт а/м и ТО','182','б/у колесо','4000,00'])
        for field in ['Приход','Контрагент','Объект','Выручка']:
            self.assertEqual(row[field], '')
        data['expense_amount'] = '9999'
        self.assertIs(prepare_expense(data, source='telegram', user_id=42, name='Другой'), first)

    def test_only_allowed_categories_and_fixed_structure(self):
        for category in ('182', '117', '185'):
            data = expense_data()
            data.update(expense_category=category, expense_structure='1201')
            row = prepare_expense(data, source='telegram', user_id=42, name='Водитель')['row']
            self.assertEqual((row['КСП'], row['Структура']), ('1202', 'ЮЛ - Контейнеры'))
        for category in ('183', '115', '124', '149', 'other'):
            data = expense_data()
            data['expense_category'] = category
            with self.assertRaises(ValueError):
                prepare_expense(data, source='telegram', user_id=42, name='Водитель')

    def test_amount_validation(self):
        for value in ['4 000,50','4\u00a0000.50','4000,5']:
            self.assertEqual(parse_amount(value),'4000,50')
        for value in ['0','-4000','NaN','1e5','4000.001','40 00','1,2,3','10000000000','']:
            with self.subTest(value=value), self.assertRaises(ValueError):
                parse_amount(value)

    def test_outbox_is_durable_idempotent_and_retries_delivery(self):
        with tempfile.TemporaryDirectory() as directory, patch('expense_outbox.state_directory',return_value=Path(directory)):
            submission = prepare_expense(expense_data(),source='max',user_id=42,name='Водитель')
            expense_outbox.enqueue_expense(submission)
            expense_outbox.enqueue_expense(submission)
            changed = deepcopy(submission);changed['row']['Расход']='5000'
            with self.assertRaises(ValueError):
                expense_outbox.enqueue_expense(changed)
            with patch('pickup_delivery.append_pickup_row',side_effect=TimeoutError()):
                with self.assertRaises(TimeoutError):
                    expense_outbox.deliver_expense_one()
            with sqlite3.connect(Path(directory)/'expenses.sqlite3') as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM expenses').fetchone()[0],1)
                self.assertEqual(db.execute('SELECT status FROM expenses').fetchone()[0],'retry')
                db.execute('UPDATE expenses SET next_attempt_at=0')
                self.assertIn('idx_expenses_due',str(db.execute("EXPLAIN QUERY PLAN SELECT id FROM expenses WHERE status='retry' AND next_attempt_at<=0").fetchall()))
            with patch('pickup_delivery.append_pickup_row') as append:
                self.assertTrue(expense_outbox.deliver_expense_one())
                self.assertFalse(expense_outbox.deliver_expense_one())
                append.assert_called_once_with(submission['row'],submission['key'],marker_kind='РАСХОД')

    def test_sheets_lost_response_keeps_one_numeric_expense_and_no_revenue(self):
        columns=['Дата','Месяц','Примечание','Объект','Выручка','Расход','Приход','Сальдо']
        sheet=Sheet();sheet.rows=[columns,['22.06.2026','=MONTH(A2)','Старый вывоз','1','=D2*100','','','=H1+G2-F2']]
        sheet.fail_append=True
        schema={'google_sheet_url':'https://docs.google.com/spreadsheets/d/sheet/edit#gid=7','google_sheet_columns':columns,'formula_columns':['Месяц','Выручка']}
        submission=prepare_expense(expense_data(),source='telegram',user_id=42,name='Водитель')
        submission['row']['_formula_columns'] = ['Месяц', 'Сальдо']  # Older queued expense.
        with patch.object(sheet, 'batch_update', wraps=sheet.batch_update) as batch, patch('pickup_delivery.get_sheets_client',return_value=sheet),patch('pickup_delivery._load_schema',return_value=schema):
            with self.assertRaises(TimeoutError):
                append_pickup_row(submission['row'],submission['key'],marker_kind='РАСХОД')
            append_pickup_row(submission['row'],submission['key'],marker_kind='РАСХОД')
        for request in batch.call_args.args[0]['requests']:
            if 'updateCells' in request:
                update = request['updateCells']
                start = update['start']['columnIndex']
                self.assertNotIn(7, range(start, start + len(update['rows'][0]['values'])))
            if 'copyPaste' in request:
                self.assertNotEqual(request['copyPaste']['destination']['startColumnIndex'], 7)
        self.assertEqual(sheet.appends,1)
        self.assertEqual(sheet.rows[-1][5],4000.0)
        self.assertEqual(sheet.rows[-1][3:5],['',''])
        self.assertEqual(sheet.rows[-1][7], '')
        self.assertIn('[РАСХОД:',sheet.rows[-1][2])

    def test_whitelist_is_checked_for_expenses(self):
        with patch.dict('os.environ',{'ALLOWED_CHAT_IDS':'-100, -200'}):
            self.assertTrue(expense_chat_allowed(-100))
            self.assertFalse(expense_chat_allowed(42))


class Router:
    def __init__(self):self.handlers={}
    def message_created(self,*args):
        def register(fn):self.handlers[fn.__name__]=fn;return fn
        return register
    message_callback=message_created


class ExpenseDialogsTest(unittest.IsolatedAsyncioTestCase):
    async def test_telegram_flow_and_confirmation_retry(self):
        data={}
        context=SimpleNamespace(user_data=data,application=SimpleNamespace(mark_data_for_update_persistence=lambda **kw:None,
            update_persistence=AsyncMock(),persistence=SimpleNamespace(flush=AsyncMock())))
        message=SimpleNamespace(text='',reply_text=AsyncMock())
        event=SimpleNamespace(message=message,effective_message=message,effective_chat=SimpleNamespace(id=1),effective_user=SimpleNamespace(id=42,full_name='Водитель'),
            callback_query=SimpleNamespace(data='',answer=AsyncMock(),edit_message_text=AsyncMock()))
        with patch.dict('os.environ',{'ALLOWED_CHAT_IDS':''}),patch('expense_report.enqueue_expense',side_effect=[TimeoutError(),None]) as enqueue,patch('expense_report.logger'):
            self.assertEqual(await expense_report.start(event,context),expense_report.CATEGORY)
            event.callback_query.data='mcategory:182';await expense_report.choose(event,context)
            message.text='4000';await expense_report.amount(event,context)
            message.text='б/у колесо';await expense_report.purpose(event,context)
            event.callback_query.data=confirmation_payload(data)
            await expense_report.confirm(event,context)
            frozen=deepcopy(data['expense_submission'])
            await expense_report.confirm(event,context)
            self.assertEqual(enqueue.call_args_list[0].args[0],frozen)
            self.assertEqual(enqueue.call_args_list[1].args[0],frozen)
            self.assertNotIn('expense_submission',data)

    async def test_max_flow_retries_same_expense_and_ignores_old_button(self):
        router=Router();register_expense_handlers(router,user_id=lambda e:42,user_name=lambda e:'Водитель',chat_id=lambda e:1)
        data={}
        async def update_data(**kw):data.update(kw)
        async def clear():data.clear()
        context=SimpleNamespace(clear=clear,set_state=AsyncMock(),update_data=update_data,get_data=AsyncMock(return_value=data))
        event=SimpleNamespace(message=SimpleNamespace(answer=AsyncMock(),body=SimpleNamespace(text='')),callback=SimpleNamespace(payload=''))
        with patch.dict('os.environ',{'ALLOWED_CHAT_IDS':''}),patch('expense_max.enqueue_expense',side_effect=[TimeoutError(),None]) as enqueue,patch('expense_max.logger'):
            h=router.handlers
            await h['expense_start'](event,context)
            event.callback.payload='mcategory:182';await h['expense_choose'](event,context)
            event.message.body.text='4000';await h['expense_text'](event,context)
            event.message.body.text='б/у колесо';await h['expense_text'](event,context)
            event.callback.payload=confirmation_payload(data)
            await h['expense_confirm'](event,context)
            frozen=deepcopy(data['expense_submission'])
            await h['expense_confirm'](event,context)
            await h['expense_confirm'](event,context)
            self.assertEqual(enqueue.call_count,2)
            self.assertEqual(enqueue.call_args_list[1].args[0],frozen)
