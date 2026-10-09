from __future__ import annotations

import asyncio
import base64
import json
import os
import sqlite3
from contextlib import closing
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path

from maxapi.context import BaseContext


def state_directory() -> Path:
    directory = Path(__file__).resolve().parent / 'data' / 'private-state'
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory.chmod(0o700)
    return directory


def encode(value):
    if isinstance(value, bytes):
        return {'__type__': 'bytes', 'value': base64.b64encode(value).decode('ascii')}
    if isinstance(value, (set, tuple)):
        return {'__type__': type(value).__name__, 'value': [encode(item) for item in value]}
    if isinstance(value, (datetime, date, time, Decimal)):
        return {'__type__': type(value).__name__, 'value': str(value)}
    if isinstance(value, dict):
        return {key: encode(item) for key, item in value.items()}
    if isinstance(value, list):
        return [encode(item) for item in value]
    return value


def decode(value):
    if isinstance(value, list):
        return [decode(item) for item in value]
    if isinstance(value, dict):
        kind = value.get('__type__')
        if kind == 'bytes':
            return base64.b64decode(value['value'])
        if kind in ('set', 'tuple'):
            return (set if kind == 'set' else tuple)(decode(item) for item in value['value'])
        parsers = {'datetime': datetime.fromisoformat, 'date': date.fromisoformat,
                   'time': time.fromisoformat, 'Decimal': Decimal}
        if kind in parsers:
            return parsers[kind](value['value'])
        return {key: decode(item) for key, item in value.items()}
    return value


class PersistentMaxContext(BaseContext):
    def __init__(self, chat_id, user_id, *, database=None, **kwargs):
        super().__init__(chat_id, user_id, **kwargs)
        self.database = Path(database) if database else state_directory() / 'max.sqlite3'
        descriptor = os.open(self.database, os.O_CREAT | os.O_WRONLY, 0o600)
        os.close(descriptor)
        self.database.chmod(0o600)
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute('CREATE TABLE IF NOT EXISTS contexts (chat TEXT, user TEXT, state TEXT, data TEXT NOT NULL, PRIMARY KEY(chat, user))')

    def _operate(self, operation, value=None):
        with closing(sqlite3.connect(self.database, timeout=30)) as connection, connection:
            connection.execute('BEGIN IMMEDIATE')
            key = (str(self.chat_id), str(self.user_id))
            row = connection.execute('SELECT state, data FROM contexts WHERE chat=? AND user=?', key).fetchone()
            state, data = (row[0], decode(json.loads(row[1]))) if row else (None, {})
            if operation == 'read':
                return state, data
            if operation == 'clear':
                connection.execute('DELETE FROM contexts WHERE chat=? AND user=?', key)
                return
            if operation == 'state':
                state = None if value is None else str(value)
            elif operation == 'set':
                data = value
            elif operation == 'update':
                data.update(value)
            connection.execute('INSERT OR REPLACE INTO contexts VALUES (?, ?, ?, ?)',
                               (*key, state, json.dumps(encode(data), ensure_ascii=False)))
            return data

    async def get_state(self):
        return (await asyncio.to_thread(self._operate, 'read'))[0]

    async def get_data(self):
        return (await asyncio.to_thread(self._operate, 'read'))[1]

    async def set_state(self, state=None):
        await asyncio.to_thread(self._operate, 'state', state)

    async def set_data(self, data):
        await asyncio.to_thread(self._operate, 'set', data)

    async def update_data(self, **kwargs):
        return await asyncio.to_thread(self._operate, 'update', kwargs)

    async def clear(self):
        await asyncio.to_thread(self._operate, 'clear')
