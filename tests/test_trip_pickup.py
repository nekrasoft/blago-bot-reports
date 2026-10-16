import unittest
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pickup_submission
import trip_report
from trip_pickup import add_trip_photos, manual_site, prepare_trip_submission
with patch('maxapi.Bot'):
    import max_bot


def trip_data():
    return {'hodka_confirmation_key': 'a' * 32, 'hodka_selected_contractor': 'Клиент', 'hodka_counterparty_id': 7,
            'hodka_trips_count': 2, 'hodka_volume_note': 'Объём: 60 м3', 'hodka_date_str': '16.10.2026',
            'hodka_site': {'district': 'Знак', 'address': 'Площадка 1'},
            'hodka_photos': [{'file_bytes': b'photo', 'file_name': 'body.jpg', 'content_type': 'image/jpeg', 'source_file_id': 'photo'}],
            'hodka_waybill': {'file_bytes': b'pdf', 'file_name': 'ticket.pdf', 'content_type': 'application/pdf', 'source_file_id': 'ticket'}}


class TripPickupTest(unittest.TestCase):
    def test_trip_row_preserves_volume_location_cash_and_file_tokens_on_retry(self):
        data = trip_data()
        data.update(hodka_selected_contractor='Частник', hodka_cash_income='7 000'.replace(' ', ''))
        submission = prepare_trip_submission(data, source='max', user_id=42, name='Водитель', chat_id=1)
        self.assertEqual(submission['request']['trip_details']['tripVolumeM3'], '60')
        self.assertEqual(submission['row']['Объект'], '2')
        self.assertEqual(submission['row']['Приход'], '7000')
        self.assertEqual(submission['row']['Выручка'], '')
        self.assertEqual(submission['row']['_skip_formula_columns'], ['Выручка'])
        self.assertIn('Знак', submission['row']['Примечание'])
        for token in submission['request']['waybill_tokens'] + submission['photo_tokens']:
            self.assertIn(token, submission['row']['Примечание'])
        data['hodka_volume_note'] = 'Объём: 36 м3'
        self.assertIs(prepare_trip_submission(data, source='max', user_id=42, name='Другой', chat_id=1), submission)
        with patch.object(pickup_submission, 'save_waybill_file') as save, patch.object(pickup_submission, 'create_pickup_report') as api:
            pickup_submission.send_submission(submission)
        self.assertEqual(save.call_count, 2)
        self.assertEqual(api.call_args.kwargs['report_type'], 'trip')
        self.assertEqual(api.call_args.kwargs['driver_user_id'], 42)

    def test_photos_accept_ten_and_reject_eleventh_and_non_image(self):
        data = {}
        files = [{'source_file_id': str(i), 'content_type': 'image/jpeg'} for i in range(10)]
        self.assertEqual(add_trip_photos(data, files), 10)
        self.assertEqual(add_trip_photos(data, files), 10)
        with self.assertRaises(ValueError):
            add_trip_photos(data, [{'source_file_id': '11', 'content_type': 'image/jpeg'}])
        self.assertEqual(len(data['hodka_photos']), 10)
        with self.assertRaises(ValueError):
            add_trip_photos({}, [{'source_file_id': 'pdf', 'content_type': 'application/pdf'}])

    def test_site_requires_district_and_address(self):
        self.assertEqual(manual_site(' Знак ; Площадка 1 '), {'district': 'Знак', 'address': 'Площадка 1'})
        with self.assertRaises(ValueError):
            manual_site('Без района')


class TripConfirmationTest(unittest.IsolatedAsyncioTestCase):
    async def test_both_bots_reuse_frozen_submission_after_lost_response(self):
        for module, handler in [(trip_report, trip_report.hodka_confirm), (max_bot, max_bot.handle_hodka_confirm)]:
            with self.subTest(bot=module.__name__):
                data = trip_data()
                async def update_data(**kwargs):
                    data.update(kwargs)
                async def clear():
                    data.clear()
                context = SimpleNamespace(user_data=data, get_data=AsyncMock(return_value=data), update_data=update_data, clear=clear,
                    application=SimpleNamespace(mark_data_for_update_persistence=lambda **kw: None,
                    update_persistence=AsyncMock(), persistence=SimpleNamespace(flush=AsyncMock())))
                event = SimpleNamespace(callback=SimpleNamespace(payload='hconfirm:'+'a'*32),
                    callback_query=SimpleNamespace(data='hconfirm:'+'a'*32, answer=AsyncMock(), edit_message_text=AsyncMock()),
                    message=SimpleNamespace(answer=AsyncMock()), effective_user=SimpleNamespace(id=42, full_name='Водитель'),
                    effective_chat=SimpleNamespace(id=1))
                with patch.object(module, 'send_submission', side_effect=[TimeoutError(), {'id': 1}]) as send, \
                     patch.object(module, 'logger'), patch.object(max_bot, '_max_user_id', return_value=42), \
                     patch.object(max_bot, '_max_user_name', return_value='Водитель'), patch.object(max_bot, '_max_chat_id', return_value=1), \
                     patch.object(max_bot, '_max_message_id', return_value='message'):
                    await handler(event, context)
                    frozen = deepcopy(data['hodka_submission'])
                    await handler(event, context)
                    self.assertEqual(send.call_args_list[0].args[0], frozen)
                    self.assertEqual(send.call_args_list[1].args[0], frozen)
                    self.assertNotIn('hodka_submission', data)

    async def test_selected_body_volume_is_multiplied_but_manual_volume_is_total(self):
        data = trip_data()
        event = SimpleNamespace(callback_query=SimpleNamespace(data='hvol:30', answer=AsyncMock(), edit_message_text=AsyncMock(),
            message=SimpleNamespace(reply_text=AsyncMock())), message=SimpleNamespace(text='55',reply_text=AsyncMock()))
        context = SimpleNamespace(user_data=data)
        with patch.object(trip_report, '_ask_cash_or_waybill', new=AsyncMock()):
            await trip_report.hodka_volume_callback(event, context)
            self.assertIn('60', data['hodka_volume_note'])
            await trip_report.hodka_save_volume(event, context)
            self.assertIn('55', data['hodka_volume_note'])


class TripDialogStagesTest(unittest.IsolatedAsyncioTestCase):
    async def test_waybill_and_cash_lead_to_photo_stage_instead_of_saving(self):
        context = SimpleNamespace(user_data=trip_data())
        message = SimpleNamespace(reply_text=AsyncMock(), text='7000')
        event = SimpleNamespace(message=message, callback_query=None)
        state = await trip_report._ask_waybill(event, context)
        self.assertEqual(state, trip_report.STATE_HODKA_FILE)
        state = await trip_report.hodka_save_cash(event, context)
        self.assertEqual(state, trip_report.STATE_HODKA_PHOTOS)
        self.assertNotIn('hodka_submission', context.user_data)


class TripApiProtocolTest(unittest.TestCase):
    def test_trip_payload_and_bunker_hash_compatibility(self):
        import json
        import map_client
        args = dict(items=[], submission_key='key', sheet_row={'Дата': '16.10.2026'}, cleanup_status='not_required',
                    cleanup_comment=None, driver_source='max', driver_user_id=42, driver_name='Водитель')
        response = SimpleNamespace(is_error=False, json=lambda: {'id': 1, 'sheetQueued': True})
        with patch.object(map_client, '_get_base_url', return_value='https://map.example.invalid'), \
             patch.object(map_client.httpx, 'post', return_value=response) as post:
            map_client.create_pickup_report(**args)
            self.assertNotIn('reportType', json.loads(post.call_args.kwargs['data']['payload']))
            map_client.create_pickup_report(**args, report_type='trip', trip_details={'tripsCount': 2, 'tripVolumeM3': '60'})
            payload = json.loads(post.call_args.kwargs['data']['payload'])
            self.assertEqual(payload['reportType'], 'trip')
            self.assertEqual(payload['tripVolumeM3'], '60')


class TripStaleConfirmationTest(unittest.IsolatedAsyncioTestCase):
    async def test_old_confirmation_does_not_send_current_report(self):
        for module, handler in [(trip_report, trip_report.hodka_confirm), (max_bot, max_bot.handle_hodka_confirm)]:
            data = trip_data()
            message = SimpleNamespace(answer=AsyncMock(), reply_text=AsyncMock())
            event = SimpleNamespace(callback=SimpleNamespace(payload='hconfirm:'+'b'*32), message=message,
                callback_query=SimpleNamespace(data='hconfirm:'+'b'*32, answer=AsyncMock(), message=message))
            context = SimpleNamespace(user_data=data, get_data=AsyncMock(return_value=data))
            with patch.object(module, 'send_submission') as send, patch.object(max_bot, '_max_user_id', return_value=42):
                await handler(event, context)
                send.assert_not_called()

    async def test_max_parallel_uploads_do_not_exceed_photo_limit(self):
        import asyncio
        data = trip_data()
        data['hodka_photos'] = []
        async def update_data(**changes):
            data.update(changes)
        context = SimpleNamespace(get_data=AsyncMock(return_value=data), update_data=update_data)
        event = SimpleNamespace(message=SimpleNamespace(body=SimpleNamespace(text='', attachments=[SimpleNamespace(type='image')]*6), answer=AsyncMock()))
        files = [{'source_file_id': str(i), 'content_type': 'image/jpeg'} for i in range(6)]
        with patch.object(max_bot, '_max_user_id', return_value=42), \
             patch.object(max_bot, '_download_max_site_photos', new=AsyncMock(return_value=files)) as download:
            await asyncio.gather(max_bot.handle_hodka_photos(event, context), max_bot.handle_hodka_photos(event, context))
            self.assertEqual(len(data['hodka_photos']), 6)
            self.assertEqual(download.call_count, 1)
