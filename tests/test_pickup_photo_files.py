from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import bunker_report
import pickup_submission

with patch("maxapi.Bot"):
    import max_bot


class PickupPhotoFilesTest(unittest.IsolatedAsyncioTestCase):
    async def test_photos_are_saved_and_linked_with_and_without_waybills(self):
        photo = {
            "file_bytes": b"photo", "file_name": "site.jpg",
            "content_type": "image/jpeg", "source_file_id": "photo-id",
        }
        waybill = {
            "file_bytes": b"waybill", "file_name": "ticket.pdf",
            "content_type": "application/pdf", "source_file_id": "ticket-id",
        }
        for module in (bunker_report, max_bot):
            for waybills in ([waybill], []):
                with self.subTest(bot=module.__name__, waybills=bool(waybills)):
                    log = [{
                        "id": "b1", "number": 1, "contractor": "Заказчик",
                        "note": "Площадка", "billing_units": "1",
                    }]
                    data = {
                        "bunker_log": log, "bunker_waybills": waybills,
                        "bunker_site_photos": [photo], "waybills": waybills,
                        "site_photos": [photo],
                    }
                    message = SimpleNamespace(answer=AsyncMock(), delete=AsyncMock())
                    event = SimpleNamespace(
                        callback=SimpleNamespace(payload="confirm"), message=message,
                        callback_query=SimpleNamespace(
                            data="confirm", answer=AsyncMock(), edit_message_text=AsyncMock(),
                        ),
                        effective_user=SimpleNamespace(id=1, full_name="Водитель"),
                        effective_chat=SimpleNamespace(id=2),
                    )
                    context = SimpleNamespace(
                        user_data=data, get_data=AsyncMock(return_value=data), clear=AsyncMock(), update_data=AsyncMock(),
                        application=SimpleNamespace(mark_data_for_update_persistence=lambda **kwargs: None,
                            update_persistence=AsyncMock(), persistence=SimpleNamespace(flush=AsyncMock())),
                    )
                    tokens = ["wb_ticket123"] * len(waybills) + ["wb_photo1234"]
                    with (
                        patch.object(pickup_submission, "save_waybill_file") as save,
                        patch.object(pickup_submission, "create_pickup_report") as report,
                        patch.object(module, "_format_bunker_report", return_value="Сохранено"),
                    ):
                        if module is bunker_report:
                            await module.confirm_pickup(event, context)
                        else:
                            with (
                                patch.object(module, "_max_chat_id", return_value=2),
                                patch.object(module, "_max_user_id", return_value=1),
                                patch.object(module, "_max_user_name", return_value="Водитель"),
                                patch.object(module, "_max_message_id", return_value="msg"),
                            ):
                                await module.handle_bunker_confirm(event, context)

                    tokens = [call.kwargs["file_token"] for call in save.call_args_list]
                    self.assertEqual(save.call_count, len(waybills) + 1)
                    self.assertEqual([call.kwargs["kind"] for call in save.call_args_list],
                                     ["container_waybill"] * len(waybills) + ["site_photo"])
                    self.assertEqual(
                        [call.kwargs["file_bytes"] for call in save.call_args_list],
                        [file["file_bytes"] for file in waybills + [photo]],
                    )
                    self.assertEqual(
                        [call.kwargs["file_token"] for call in save.call_args_list], tokens,
                    )
                    self.assertEqual(report.call_args.kwargs["waybill_tokens"], tokens[:-1])
                    self.assertEqual(report.call_args.kwargs["site_photos"], [photo])
                    note = report.call_args.kwargs["sheet_row"]["Примечание"]
                    for token in tokens:
                        self.assertIn(f"[ПЛ:{token}]", note)


if __name__ == "__main__":
    unittest.main()
