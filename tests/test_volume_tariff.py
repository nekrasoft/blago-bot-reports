import unittest
from unittest.mock import patch, Mock
from decimal import Decimal

from configure_volume_billing import propose_tariff
from pickup_delivery import append_pickup_row, apply_volume_tariff
from test_pickup_delivery import Sheet


class VolumeTariffTest(unittest.TestCase):
    def book(self, price='1200'):
        book = Mock()
        book.worksheet.return_value.get_all_values.return_value = [
            ['Контрагент', 'Цена', 'Единица тарифа', 'Тариф за м³'],
            ['Клиент', '36000', 'м³', price],
        ]
        return book

    def row(self, volume=30):
        return {'Дата': '17.10.2026', 'Контрагент': 'Клиент', 'Объект': '1',
                'Примечание': 'Площадка', 'Единица тарифа': 'м³', 'Объём, м³': str(volume)}

    def test_proposed_rates_use_exact_division_and_explicit_formula(self):
        self.assertEqual(propose_tariff('42000')[0], '1400')
        self.assertEqual(propose_tariff('51120', '=1420*36')[0], '1420')
        self.assertEqual(propose_tariff('46800')[0], '1560')
        self.assertIn('также на 36', propose_tariff('46800')[1])
        self.assertIsNone(propose_tariff('54800')[0])

    def test_30_36_and_mixed_volume(self):
        for volume, total in ((30, '36000.00'), (36, '43200.00'), (66, '79200.00')):
            row = apply_volume_tariff(self.book(), self.row(volume))
            self.assertEqual(row['Выручка'], total)
            self.assertEqual(row['Тариф на дату вывоза'], '1200.00')
            self.assertEqual(row['Объект'], '1')
        for tariff in ('', 'NaN', '-1', '0', '0.001'):
            with self.assertRaises(ValueError):
                apply_volume_tariff(self.book(tariff), self.row())

    def test_snapshot_formula_and_retry_do_not_read_changed_tariff(self):
        columns = ['Дата', 'Месяц', 'Примечание', 'Объект', 'Выручка', 'Объём, м³', 'Единица тарифа', 'Тариф на дату вывоза', 'Контрагент']
        sheet = Sheet()
        sheet.rows = [columns, ['16.10.2026', '=MONTH(A2)', 'Старый', '1', '=D2*100', '', '', '', 'Старый клиент']]
        sheet.row_values = lambda _: columns
        book = self.book()
        sheet.worksheet = book.worksheet
        sheet.fail_append = True
        schema = {'google_sheet_url': 'https://docs.google.com/spreadsheets/d/s/edit#gid=7', 'google_sheet_columns': columns}
        with patch('pickup_delivery.get_sheets_client', return_value=sheet), patch('pickup_delivery._load_schema', return_value=schema):
            with self.assertRaises(TimeoutError):
                append_pickup_row(self.row(36), 'volume-id')
            book.worksheet.return_value.get_all_values.side_effect = AssertionError('Must not reread a delivered tariff')
            append_pickup_row(self.row(36), 'volume-id')
        self.assertEqual(sheet.appends, 1)
        self.assertEqual(sheet.rows[-1][3], 1.0)
        self.assertTrue(sheet.rows[-1][4].startswith('=IF(G3="м³";ROUND(F3*H3;2);D3*IF(VLOOKUP('))
        self.assertEqual(sheet.rows[-1][5], 36.0)
        self.assertEqual(sheet.rows[-1][7], 1200.0)
