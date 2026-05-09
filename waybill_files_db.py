from __future__ import annotations

import hashlib
import os
from datetime import datetime
from functools import lru_cache


def _get_database_url() -> str:
    host = os.environ.get("MYSQL_HOST", "localhost")
    port = os.environ.get("MYSQL_PORT", "3306")
    user = os.environ.get("MYSQL_USER", "tbank_service")
    password = os.environ.get("MYSQL_PASSWORD", "")
    database = os.environ.get("MYSQL_DATABASE", "tbank_invoicing")
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"


@lru_cache(maxsize=1)
def _get_engine():
    import hashlib as _hashlib

    from sqlalchemy import create_engine

    orig_md5 = _hashlib.md5

    def patched_md5(data: bytes = b"", *, usedforsecurity: bool = True):
        try:
            return orig_md5(data, usedforsecurity=usedforsecurity)
        except TypeError:
            return _hashlib.new("md5", data)

    _hashlib.md5 = patched_md5  # type: ignore[assignment]
    return create_engine(
        _get_database_url(),
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=os.environ.get("SQL_ECHO", "").lower() in {"1", "true", "yes"},
    )


@lru_cache(maxsize=1)
def _get_works_files_table():
    from sqlalchemy import BigInteger, Column, DateTime, Integer, LargeBinary, MetaData, String, Table

    metadata = MetaData()
    return Table(
        "works_files",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("file_token", String(64), nullable=False),
        Column("work_id", Integer, nullable=True),
        Column("source", String(20), nullable=False),
        Column("source_chat_id", String(64), nullable=True),
        Column("source_user_id", String(64), nullable=True),
        Column("source_message_id", String(128), nullable=True),
        Column("source_file_id", String(512), nullable=True),
        Column("file_name", String(255), nullable=True),
        Column("content_type", String(100), nullable=True),
        Column("file_size", BigInteger, nullable=False),
        Column("file_sha256", String(64), nullable=False),
        Column("file_data", LargeBinary, nullable=False),
        Column("created_at", DateTime, nullable=False),
        Column("linked_at", DateTime, nullable=True),
    )


def save_waybill_file(
    *,
    file_token: str,
    source: str,
    file_bytes: bytes,
    source_chat_id: str | int | None = None,
    source_user_id: str | int | None = None,
    source_message_id: str | int | None = None,
    source_file_id: str | None = None,
    file_name: str | None = None,
    content_type: str | None = None,
) -> str:
    digest = hashlib.sha256(file_bytes).hexdigest()
    values = {
        "file_token": file_token,
        "source": source,
        "source_chat_id": _optional_str(source_chat_id, max_len=64),
        "source_user_id": _optional_str(source_user_id, max_len=64),
        "source_message_id": _optional_str(source_message_id, max_len=128),
        "source_file_id": _optional_str(source_file_id, max_len=512),
        "file_name": _optional_str(file_name, max_len=255),
        "content_type": _optional_str(content_type, max_len=100),
        "file_size": len(file_bytes),
        "file_sha256": digest,
        "file_data": file_bytes,
        "created_at": datetime.utcnow(),
        "linked_at": None,
    }

    from sqlalchemy.dialects.mysql import insert as mysql_insert

    table = _get_works_files_table()
    engine = _get_engine()
    stmt = mysql_insert(table).values(**values)
    stmt = stmt.on_duplicate_key_update(
        source=stmt.inserted.source,
        source_chat_id=stmt.inserted.source_chat_id,
        source_user_id=stmt.inserted.source_user_id,
        source_message_id=stmt.inserted.source_message_id,
        source_file_id=stmt.inserted.source_file_id,
        file_name=stmt.inserted.file_name,
        content_type=stmt.inserted.content_type,
        file_size=stmt.inserted.file_size,
        file_sha256=stmt.inserted.file_sha256,
        file_data=stmt.inserted.file_data,
    )
    with engine.begin() as conn:
        conn.execute(stmt)
    return digest


def _optional_str(value: str | int | None, *, max_len: int | None = None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if max_len is not None:
        text = text[:max_len]
    return text or None
