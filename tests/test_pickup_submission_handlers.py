import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import bunker_report
with patch('maxapi.Bot'):
    import max_bot


class PickupSubmissionHandlersTest(unittest.IsolatedAsyncioTestCase):
    async def test_lost_response_keeps_same_submission_in_both_bots(self):
        for module in (bunker_report, max_bot):
            with self.subTest(bot=module.__name__):
                data = {'bunker_log': [{'id': 'b1', 'number': 1, 'contractor': 'Клиент', 'note': 'Площадка', 'billing_units': 1.5}]}
                async def update_data(**changes):
                    data.update(changes)
                async def clear():
                    data.clear()
                context = SimpleNamespace(user_data=data, get_data=AsyncMock(return_value=data), update_data=update_data, clear=clear,
                    application=SimpleNamespace(mark_data_for_update_persistence=lambda **kwargs: None,
                        update_persistence=AsyncMock(), persistence=SimpleNamespace(flush=AsyncMock())))
                event = SimpleNamespace(callback=SimpleNamespace(payload='confirm'),
                    callback_query=SimpleNamespace(data='confirm', answer=AsyncMock(), edit_message_text=AsyncMock()),
                    message=SimpleNamespace(answer=AsyncMock(), delete=AsyncMock()),
                    effective_user=SimpleNamespace(id=1, full_name='Водитель'), effective_chat=SimpleNamespace(id=2))
                with patch.object(module, 'logger'), patch.object(module, 'send_submission', side_effect=[TimeoutError(), {'id': 1}]) as send, \
                     patch.object(max_bot, '_max_user_id', return_value=1), patch.object(max_bot, '_max_user_name', return_value='Водитель'), \
                     patch.object(max_bot, '_max_chat_id', return_value=2), patch.object(max_bot, '_max_message_id', return_value='msg'):
                    handler = module.confirm_pickup if module is bunker_report else module.handle_bunker_confirm
                    await handler(event, context)
                    frozen = data.get('bunker_submission') or data['submission']
                    self.assertEqual(frozen['request']['driver_user_id'], 1)
                    await handler(event, context)
                    self.assertEqual(send.call_args_list[0], send.call_args_list[1])
                    self.assertNotIn('bunker_submission', data)
                    self.assertNotIn('submission', data)
