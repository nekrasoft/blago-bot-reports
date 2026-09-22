import unittest

from bunker_pickup import build_pickup_sheet_row, pickup_group_key


class BunkerPickupTest(unittest.TestCase):
    def test_builds_fractional_sheet_quantity_and_waybill_marker(self):
        row = build_pickup_sheet_row(
            [
                {"contractor": "Заказчик", "note": "Знак", "number": 1, "billing_units": "1"},
                {"contractor": "Заказчик", "note": "Знак", "number": 2, "billing_units": "1.5"},
            ],
            "22.09.2026",
            "wb_12345678",
        )

        self.assertEqual(row["Объект"], "2,5")
        self.assertIn("# 1,2", row["Примечание"])
        self.assertIn("[ПЛ:wb_12345678]", row["Примечание"])

    def test_group_key_separates_sites_of_same_counterparty(self):
        self.assertNotEqual(
            pickup_group_key({"counterpartyId": 1, "district": "Знак"}),
            pickup_group_key({"counterpartyId": 1, "district": "Инноград"}),
        )


if __name__ == "__main__":
    unittest.main()
