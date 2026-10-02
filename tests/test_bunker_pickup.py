import json
import os
import unittest
from unittest.mock import MagicMock, patch

from bunker_pickup import build_pickup_sheet_row, pickup_group_key
from map_client import create_pickup_report


class BunkerPickupTest(unittest.TestCase):
    def test_builds_fractional_sheet_quantity_and_waybill_markers(self):
        row = build_pickup_sheet_row(
            [
                {"contractor": "Заказчик", "note": "Знак", "number": 1, "billing_units": "1"},
                {"contractor": "Заказчик", "note": "Знак", "number": 2, "billing_units": "1.5"},
            ],
            "22.09.2026",
            ["wb_12345678", "wb_87654321"],
        )

        self.assertEqual(row["Объект"], "2,5")
        self.assertIn("# 1,2", row["Примечание"])
        self.assertIn("[ПЛ:wb_12345678]", row["Примечание"])
        self.assertIn("[ПЛ:wb_87654321]", row["Примечание"])

    def test_group_key_separates_sites_of_same_counterparty(self):
        self.assertNotEqual(
            pickup_group_key({"counterpartyId": 1, "district": "Знак"}),
            pickup_group_key({"counterpartyId": 1, "district": "Инноград"}),
        )

    @patch.dict(os.environ, {"MAP_SERVICE_URL": "https://map.example", "MAP_BOT_API_KEY": "test"})
    @patch("map_client.httpx")
    def test_sends_multiple_waybills_to_map_api(self, httpx: MagicMock):
        httpx.post.return_value.is_error = False
        httpx.post.return_value.json.return_value = {"id": 1}

        create_pickup_report(
            items=[{"bunkerId": "b1", "billingUnits": 1}],
            cleanup_status="cleaned",
            cleanup_comment=None,
            driver_source="telegram",
            driver_user_id=1,
            driver_name="Водитель",
            waybills=[
                {"file_name": "one.jpg", "file_bytes": b"one", "content_type": "image/jpeg"},
                {"file_name": "two.pdf", "file_bytes": b"two", "content_type": "application/pdf"},
            ],
            waybill_tokens=["wb_12345678", "wb_87654321"],
        )

        request = httpx.post.call_args.kwargs
        self.assertEqual(
            json.loads(request["data"]["payload"])["waybillTokens"],
            ["wb_12345678", "wb_87654321"],
        )
        self.assertEqual([field for field, _ in request["files"]], ["waybills[]", "waybills[]"])


if __name__ == "__main__":
    unittest.main()
