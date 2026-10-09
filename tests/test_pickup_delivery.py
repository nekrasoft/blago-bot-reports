import unittest
from unittest.mock import patch

from pickup_submission import prepare_submission, send_submission
from pickup_delivery import append_pickup_row


class Sheet:
    id = 7
    def __init__(self):
        self.rows = [['Дата', 'Месяц', 'Примечание', 'Объект', 'Выручка'],
                     ['08.10.2026', '=MONTH(A2)', 'Старый вывоз', '1', '=D2*100']]
        self.fail_append = False
        self.fail_copy = False
        self.appends = 0
        self.copies = 0
    def col_values(self, index):
        return [row[index - 1] for row in self.rows]
    def get(self, cell_range, **kwargs):
        index = ord(cell_range[0]) - ord('A')
        return [[row[index]] for row in self.rows]
    def batch_update(self, body):
        self.copies += 1
        if self.fail_copy:
            self.fail_copy = False
            raise TimeoutError('Batch rejected before commit')
        for request in body['requests']:
            if 'insertDimension' in request:
                index = request['insertDimension']['range']['startIndex']
                self.rows.insert(index, [''] * 5)
                self.appends += 1
            elif 'updateCells' in request:
                update = request['updateCells']
                self.rows[update['start']['rowIndex']] = [next(iter(cell['userEnteredValue'].values())) for cell in update['rows'][0]['values']]
            elif 'copyPaste' in request:
                copy = request['copyPaste']
                source, target = copy['source'], copy['destination']
                self.rows[target['startRowIndex']][target['startColumnIndex']] = self.rows[source['startRowIndex']][source['startColumnIndex']]
        if self.fail_append:
            self.fail_append = False
            raise TimeoutError('Response lost after commit')
    def open_by_key(self, key):
        return self
    def get_worksheet_by_id(self, gid):
        return self
    def set_timeout(self, timeout):
        pass


class PickupDeliveryTest(unittest.TestCase):
    def test_lost_response_and_rejected_batch_do_not_duplicate_rows(self):
        schema = {'google_sheet_url': 'https://docs.google.com/spreadsheets/d/sheet/edit#gid=7',
                  'google_sheet_columns': ['Дата', 'Месяц', 'Примечание', 'Объект', 'Выручка'],
                  'formula_columns': ['Месяц', 'Выручка']}
        row = {'Дата': '09.10.2026', 'Месяц': '10', 'Примечание': 'Площадка', 'Объект': '1,5'}
        for stage in ('append', 'copy'):
            with self.subTest(stage=stage):
                sheet = Sheet()
                setattr(sheet, 'fail_' + stage, True)
                with patch('pickup_delivery.get_sheets_client', return_value=sheet), patch('pickup_delivery._load_schema', return_value=schema):
                    with self.assertRaises(TimeoutError):
                        append_pickup_row(row, '12345678-1234-1234-1234-123456789abc')
                    append_pickup_row(row, '12345678-1234-1234-1234-123456789abc')
                    append_pickup_row(row, '12345678-1234-1234-1234-123456789abc')
                self.assertEqual(sheet.appends, 1)
                self.assertEqual(sheet.rows[-1][3], 1.5)
                self.assertTrue(sheet.rows[-1][1].startswith('='))
                self.assertTrue(sheet.rows[-1][4].startswith('='))

    def test_retry_preserves_payload_tokens_date_and_driver(self):
        data = {'bunker_log': [{'id': 'b1', 'number': 1, 'contractor': 'Клиент', 'note': 'Адрес', 'billing_units': 1.5}],
                'waybills': [{'file_bytes': b'pdf', 'file_name': 'ticket.pdf', 'content_type': 'application/pdf'}]}
        first = prepare_submission(data, source='max', user_id=1, name='Водитель', chat_id=2)
        data['bunker_log'][0]['billing_units'] = 2
        again = prepare_submission(data, source='max', user_id=1, name='Новое имя', chat_id=2)
        self.assertEqual(first, again)
        self.assertEqual(first['request']['items'][0]['billingUnits'], 1.5)
        self.assertEqual(first['request']['driver_name'], 'Водитель')
        with patch('pickup_submission.save_waybill_file') as save, patch('pickup_submission.create_pickup_report', return_value={'id': 3}) as create:
            send_submission(first)
            send_submission(again)
        self.assertEqual(create.call_args_list[0], create.call_args_list[1])
        self.assertEqual(save.call_args_list[0], save.call_args_list[1])
        self.assertEqual(create.call_args.kwargs['sheet_row']['Объект'], '1,5')
