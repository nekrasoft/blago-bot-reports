from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pickup_submission

with patch("maxapi.Bot"):
    import max_bot


class MaxDriverIdentityTest(unittest.IsolatedAsyncioTestCase):
    async def test_pickup_report_uses_callback_user_instead_of_bot_sender(self):
        event = SimpleNamespace(
            callback=SimpleNamespace(
                payload="confirm",
                user=SimpleNamespace(
                    user_id=1,
                    first_name="Иван",
                    last_name="Иванов",
                ),
            ),
            message=SimpleNamespace(
                sender=SimpleNamespace(
                    user_id=999,
                    first_name="ReportsBot",
                    last_name="",
                ),
                answer=AsyncMock(),
                delete=AsyncMock(),
            ),
        )
        context = SimpleNamespace(
            get_data=AsyncMock(return_value={
                "bunker_log": [{
                    "id": "b1",
                    "number": 1,
                    "contractor": "Заказчик",
                    "note": "Площадка",
                    "billing_units": "1",
                }],
            }),
            clear=AsyncMock(),
            update_data=AsyncMock(),
        )

        with (
            patch.object(pickup_submission, "create_pickup_report") as create_report,
            patch.object(max_bot, "append_rows"),
            patch.object(max_bot, "_format_bunker_report", return_value="Сохранено"),
        ):
            await max_bot.handle_bunker_confirm(event, context)

        self.assertEqual(create_report.call_args.kwargs["driver_user_id"], 1)
        self.assertEqual(create_report.call_args.kwargs["driver_name"], "Иван Иванов")


if __name__ == "__main__":
    unittest.main()
