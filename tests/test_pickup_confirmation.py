import unittest
from bunker_pickup import pickup_confirmation

class PickupConfirmationTest(unittest.TestCase):
    def test_fractional_units_are_distinct_from_physical_count(self):
        text = pickup_confirmation([
            {'number': 1, 'contractor': 'Клиент', 'address': 'Площадка', 'volume': 8,
             'billing_units': 1.5, 'requiresContainerWaybill': True},
            {'number': 2, 'volume': 8, 'billing_units': 1},
        ], 'not_cleaned', 'Нет доступа', [], [], 'Мастер отсутствовал')
        for expected in ['Вывезено: 2', 'К оплате: 2,5', 'Расчётный объём: 20 м³',
                         'Нет доступа', 'Мастер отсутствовал', 'Площадка']:
            self.assertIn(expected, text)
