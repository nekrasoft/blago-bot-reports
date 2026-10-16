"""Durable local delivery queue for driver cash expenses."""
from contextlib import closing
from hashlib import sha256
import json
import os
import sqlite3
import time
from uuid import uuid4

from persistent_context import state_directory


def connect():
    path = state_directory() / 'expenses.sqlite3'
    descriptor = os.open(path, os.O_CREAT | os.O_WRONLY, 0o600)
    os.close(descriptor)
    path.chmod(0o600)
    connection = sqlite3.connect(path, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.executescript('''CREATE TABLE IF NOT EXISTS expenses (
        id INTEGER PRIMARY KEY, submission_key TEXT NOT NULL UNIQUE, payload TEXT NOT NULL,
        payload_hash TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
        next_attempt_at REAL NOT NULL, lease_token TEXT, attempts INTEGER NOT NULL DEFAULT 0);
        CREATE INDEX IF NOT EXISTS idx_expenses_due ON expenses(status, next_attempt_at, id);''')
    return connection


def enqueue_expense(submission: dict) -> None:
    payload = json.dumps(submission, ensure_ascii=False, sort_keys=True)
    digest = sha256(payload.encode()).hexdigest()
    with closing(connect()) as connection, connection:
        connection.execute('BEGIN IMMEDIATE')
        existing = connection.execute('SELECT payload_hash FROM expenses WHERE submission_key=?', (submission['key'],)).fetchone()
        if existing is not None:
            if existing['payload_hash'] != digest:
                raise ValueError('Состав расхода изменился после подтверждения')
            return
        connection.execute('INSERT INTO expenses(submission_key,payload,payload_hash,next_attempt_at) VALUES(?,?,?,?)',
                           (submission['key'], payload, digest, time.time()))


def deliver_expense_one() -> bool:
    from pickup_delivery import append_pickup_row
    with closing(connect()) as connection, connection:
        connection.execute('BEGIN IMMEDIATE')
        job = connection.execute("SELECT id,payload FROM expenses WHERE status IN ('pending','retry','sending') AND next_attempt_at<=? ORDER BY id LIMIT 1", (time.time(),)).fetchone()
        if job is None:
            return False
        token = uuid4().hex
        connection.execute("UPDATE expenses SET status='sending',next_attempt_at=?,lease_token=?,attempts=attempts+1 WHERE id=?", (time.time()+600, token, job['id']))
    submission = json.loads(job['payload'])
    sent = False
    try:
        append_pickup_row(submission['row'], submission['key'], marker_kind='РАСХОД')
        sent = True
    finally:
        with closing(connect()) as connection, connection:
            connection.execute('UPDATE expenses SET status=?,next_attempt_at=?,lease_token=NULL WHERE id=? AND lease_token=?',
                               ('sent' if sent else 'retry', time.time()+60, job['id'], token))
    return True
