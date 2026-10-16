"""Telegram /m dialog. Accepted expenses are delivered by the shared worker."""
import asyncio
import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackQueryHandler, CommandHandler, ConversationHandler, MessageHandler, filters

from expense import CATEGORIES, parse_amount, set_purpose, expense_summary, confirmation_payload, prepare_expense, expense_chat_allowed
from expense_outbox import enqueue_expense

CATEGORY, AMOUNT, PURPOSE, CONFIRM = range(1, 5)
logger = logging.getLogger(__name__)


def clear_expense(data):
    for key in list(data):
        if key.startswith('expense_'):
            data.pop(key, None)


def keyboard(options):
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data=payload)] for label, payload in options]
                                + [[InlineKeyboardButton('Отмена', callback_data='mcancel')]])


async def allowed(update):
    if expense_chat_allowed(update.effective_chat.id):
        return True
    await update.effective_message.reply_text('Запись расходов недоступна в этом чате.')
    return False


async def start(update, context):
    """Команда /m — расход водителя."""
    if not await allowed(update):
        return ConversationHandler.END
    clear_expense(context.user_data)
    await update.message.reply_text('Расход. Выберите категорию:', reply_markup=keyboard([(v, 'mcategory:'+k) for k, v in CATEGORIES.items()]))
    return CATEGORY


async def choose(update, context):
    if not await allowed(update):
        return ConversationHandler.END
    query = update.callback_query
    await query.answer()
    if query.data == 'mcancel':
        clear_expense(context.user_data)
        await query.edit_message_text('Отменено.')
        return ConversationHandler.END
    kind, value = query.data.split(':', 1)
    if kind == 'mcategory' and value in CATEGORIES:
        context.user_data['expense_category'] = value
        await query.edit_message_text('Сколько потратили? Введите сумму в рублях, например: 4000 или 4 000,50.')
        return AMOUNT
    return CATEGORY


async def amount(update, context):
    if not await allowed(update):
        return ConversationHandler.END
    try:
        context.user_data['expense_amount'] = parse_amount(update.message.text or '')
    except ValueError as e:
        await update.message.reply_text(str(e))
        return AMOUNT
    await update.message.reply_text('На что потратили деньги? Например: б/у колесо.')
    return PURPOSE


async def purpose(update, context):
    if not await allowed(update):
        return ConversationHandler.END
    try:
        set_purpose(context.user_data, update.message.text or '')
    except ValueError as e:
        await update.message.reply_text(str(e))
        return PURPOSE
    await update.message.reply_text(expense_summary(context.user_data), reply_markup=keyboard([('Подтвердить расход', confirmation_payload(context.user_data))]))
    return CONFIRM


async def confirm(update, context):
    if not await allowed(update):
        return ConversationHandler.END
    query = update.callback_query
    await query.answer()
    if query.data == 'mcancel':
        return await cancel(update, context)
    if query.data != confirmation_payload(context.user_data):
        return CONFIRM
    try:
        user = update.effective_user
        submission = prepare_expense(context.user_data, source='telegram', user_id=user.id, name=user.full_name)
        context.application.mark_data_for_update_persistence(user_ids=user.id)
        await context.application.update_persistence()
        await context.application.persistence.flush()
        await asyncio.to_thread(enqueue_expense, submission)
    except Exception:
        logger.exception('Не удалось сохранить расход')
        await query.edit_message_text('Не удалось подтвердить запись расхода. Повторите — дубликат не создастся.',
            reply_markup=keyboard([('Повторить', confirmation_payload(context.user_data))]))
        return CONFIRM
    await query.edit_message_text('Расход принят.\n\n'+expense_summary(context.user_data))
    clear_expense(context.user_data)
    return ConversationHandler.END


async def cancel(update, context):
    clear_expense(context.user_data)
    if update.callback_query:
        await update.callback_query.edit_message_text('Отменено.')
    else:
        await update.message.reply_text('Отменено.')
    return ConversationHandler.END


def get_expense_conversation_handler():
    return ConversationHandler(entry_points=[CommandHandler('m', start)], states={
        CATEGORY: [CallbackQueryHandler(choose, pattern=r'^(mcategory:(182|117|185)|mcancel)$')],
        AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, amount)],
        PURPOSE: [MessageHandler(filters.TEXT & ~filters.COMMAND, purpose)],
        CONFIRM: [CallbackQueryHandler(confirm, pattern=r'^(mconfirm:[a-f0-9]{32}|mcancel)$')],
    }, fallbacks=[CommandHandler('cancel', cancel)], name='expense_report', persistent=True, per_chat=False, per_user=True)
