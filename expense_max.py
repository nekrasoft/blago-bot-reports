"""MAX /m UI; the expense model and outbox are shared with Telegram."""
import asyncio
from functools import wraps
import logging
from weakref import WeakValueDictionary

from maxapi.context import State, StatesGroup
from maxapi.types import CallbackButton, Command
from maxapi.utils.inline_keyboard import InlineKeyboardBuilder

from expense import CATEGORIES, parse_amount, set_purpose, expense_summary, confirmation_payload, prepare_expense, expense_chat_allowed
from expense_outbox import enqueue_expense

logger = logging.getLogger(__name__)


class ExpenseDialog(StatesGroup):
    category = State()
    amount = State()
    purpose = State()
    confirming = State()


def keyboard(options):
    builder = InlineKeyboardBuilder()
    for label, payload in options:
        builder.row(CallbackButton(text=label, payload=payload))
    builder.row(CallbackButton(text='Отмена', payload='mcancel'))
    return builder.as_markup()


def register_expense_handlers(dp, *, user_id, user_name, chat_id):
    locks = WeakValueDictionary()
    def protected(handler):
        @wraps(handler)
        async def wrapped(event, context):
            if not expense_chat_allowed(chat_id(event)):
                await event.message.answer('Запись расходов недоступна в этом чате.')
                return
            lock = locks.setdefault(user_id(event), asyncio.Lock())
            async with lock:
                return await handler(event, context)
        return wrapped

    @dp.message_created(Command('m'))
    @protected
    async def expense_start(event, context):
        """Расход: сумма потраченных денег и назначение."""
        await context.clear()
        await context.set_state(ExpenseDialog.category)
        await event.message.answer('Расход. Выберите категорию:', attachments=[keyboard([(v, 'mcategory:'+k) for k, v in CATEGORIES.items()])])

    @dp.message_callback(ExpenseDialog.category)
    @protected
    async def expense_choose(event, context):
        payload = event.callback.payload or ''
        if payload == 'mcancel':
            await context.clear()
            await event.message.answer('Отменено.')
            return
        kind, _, value = payload.partition(':')
        if kind == 'mcategory' and value in CATEGORIES:
            await context.update_data(expense_category=value)
            await context.set_state(ExpenseDialog.amount)
            await event.message.answer('Сколько потратили? Введите сумму в рублях, например: 4000 или 4 000,50.')

    @dp.message_created(ExpenseDialog.amount)
    @dp.message_created(ExpenseDialog.purpose)
    @protected
    async def expense_text(event, context):
        text = str(getattr(getattr(event.message, 'body', None), 'text', '') or '').strip()
        if text.lower() in {'отмена', 'cancel', '/cancel'}:
            await context.clear()
            await event.message.answer('Отменено.')
            return
        data = await context.get_data()
        try:
            if not data.get('expense_amount'):
                await context.update_data(expense_amount=parse_amount(text))
                await context.set_state(ExpenseDialog.purpose)
                await event.message.answer('На что потратили деньги? Например: б/у колесо.')
            else:
                set_purpose(data, text)
                payload = confirmation_payload(data)
                await context.update_data(expense_purpose=data['expense_purpose'], expense_confirmation_key=data['expense_confirmation_key'])
                await context.set_state(ExpenseDialog.confirming)
                await event.message.answer(expense_summary(data), attachments=[keyboard([('Подтвердить расход', payload)])])
        except ValueError as e:
            await event.message.answer(str(e))

    @dp.message_callback(ExpenseDialog.confirming)
    @protected
    async def expense_confirm(event, context):
        payload = event.callback.payload or ''
        if payload == 'mcancel':
            await context.clear()
            await event.message.answer('Отменено.')
            return
        data = await context.get_data()
        if not data.get('expense_purpose') or payload != confirmation_payload(data):
            return
        try:
            submission = prepare_expense(data, source='max', user_id=user_id(event), name=user_name(event))
            await context.update_data(expense_submission=submission)
            await asyncio.to_thread(enqueue_expense, submission)
        except Exception:
            logger.exception('Не удалось сохранить расход')
            await event.message.answer('Не удалось подтвердить запись расхода. Повторите — дубликат не создастся.',
                attachments=[keyboard([('Повторить', confirmation_payload(data))])])
            return
        await event.message.answer('Расход принят.\n\n'+expense_summary(data))
        await context.clear()


    @dp.message_created(ExpenseDialog.category)
    @dp.message_created(ExpenseDialog.confirming)
    @protected
    async def expense_cancel(event, context):
        text = str(getattr(getattr(event.message, 'body', None), 'text', '') or '').lower().strip()
        if text in {'отмена', 'cancel', '/cancel'}:
            await context.clear()
            await event.message.answer('Отменено.')
        else:
            await event.message.answer('Используйте кнопки сообщения; для отмены отправьте /cancel.')
