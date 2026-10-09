import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import bunker_report

with patch('maxapi.Bot'):
    import max_bot


class PickupEditTest(unittest.IsolatedAsyncioTestCase):
    async def test_editing_cleanup_preserves_photos_and_clears_old_comment(self):
        data = {'bunker_site_photos': [b'photo'], 'bunker_cleanup_comment': 'old'}
        query = SimpleNamespace(data='edit:cleanup', answer=AsyncMock(), edit_message_text=AsyncMock())
        state = await bunker_report.edit_pickup(SimpleNamespace(callback_query=query), SimpleNamespace(user_data=data))
        self.assertEqual(state, bunker_report.STATE_CLEANUP)
        self.assertEqual(data['bunker_site_photos'], [b'photo'])
        self.assertNotIn('bunker_cleanup_comment', data)

        context = SimpleNamespace(update_data=AsyncMock(), set_state=AsyncMock())
        event = SimpleNamespace(message=SimpleNamespace(answer=AsyncMock()))
        await max_bot._edit_max_pickup(event, context, 'cleanup')
        context.update_data.assert_awaited_once_with(cleanup_comment=None)
        context.set_state.assert_awaited_once_with(max_bot.BunkerDialog.selecting_cleanup)

    async def test_removing_photo_keeps_other_attachments(self):
        data = {'bunker_site_photos': [{'file_name': 'one'}, {'file_name': 'two'}],
                'bunker_waybills': [{'file_name': 'ticket'}]}
        query = SimpleNamespace(data='file:remove:photos:0', answer=AsyncMock())
        with patch.object(bunker_report, '_show_confirmation', new=AsyncMock()):
            await bunker_report.manage_pickup_file(SimpleNamespace(callback_query=query), SimpleNamespace(user_data=data))
        self.assertEqual(data['bunker_site_photos'], [{'file_name': 'two'}])
        self.assertEqual(data['bunker_waybills'], [{'file_name': 'ticket'}])
