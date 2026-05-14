from __future__ import annotations

from datetime import date, datetime, time
from functools import lru_cache

from waybill_files_db import _get_engine, _optional_str


@lru_cache(maxsize=1)
def _get_driver_work_time_table():
    from sqlalchemy import Column, Date, DateTime, Integer, MetaData, String, Table
    from sqlalchemy import Time
    from sqlalchemy import UniqueConstraint

    metadata = MetaData()
    return Table(
        "driver_work_time",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("source", String(20), nullable=False),
        Column("source_chat_id", String(64), nullable=True),
        Column("source_user_id", String(64), nullable=False),
        Column("source_user_name", String(255), nullable=True),
        Column("work_date", Date, nullable=False),
        Column("start_time", Time, nullable=False),
        Column("end_time", Time, nullable=False),
        Column("duration_minutes", Integer, nullable=False),
        Column("raw_start_text", String(50), nullable=True),
        Column("raw_end_text", String(50), nullable=True),
        Column("created_at", DateTime, nullable=False),
        Column("updated_at", DateTime, nullable=False),
        UniqueConstraint(
            "source",
            "source_user_id",
            "work_date",
            name="uq_driver_work_time_source_user_date",
        ),
    )


def _ensure_driver_work_time_table() -> None:
    table = _get_driver_work_time_table()
    engine = _get_engine()
    table.metadata.create_all(engine, tables=[table])


def get_driver_work_time(
    *,
    source: str,
    source_user_id: str | int,
    work_date: date,
) -> dict | None:
    from sqlalchemy import select

    _ensure_driver_work_time_table()
    table = _get_driver_work_time_table()
    stmt = select(table).where(
        table.c.source == source,
        table.c.source_user_id == str(source_user_id),
        table.c.work_date == work_date,
    )
    engine = _get_engine()
    with engine.begin() as conn:
        row = conn.execute(stmt).mappings().first()
    return dict(row) if row else None


def save_driver_work_time(
    *,
    source: str,
    source_chat_id: str | int | None,
    source_user_id: str | int,
    source_user_name: str | None,
    work_date: date,
    start_time: time,
    end_time: time,
    duration_minutes: int,
    raw_start_text: str,
    raw_end_text: str,
) -> None:
    from sqlalchemy.dialects.mysql import insert as mysql_insert

    _ensure_driver_work_time_table()
    table = _get_driver_work_time_table()
    now = datetime.utcnow()
    values = {
        "source": source,
        "source_chat_id": _optional_str(source_chat_id, max_len=64),
        "source_user_id": _optional_str(source_user_id, max_len=64),
        "source_user_name": _optional_str(source_user_name, max_len=255),
        "work_date": work_date,
        "start_time": start_time,
        "end_time": end_time,
        "duration_minutes": duration_minutes,
        "raw_start_text": _optional_str(raw_start_text, max_len=50),
        "raw_end_text": _optional_str(raw_end_text, max_len=50),
        "created_at": now,
        "updated_at": now,
    }

    stmt = mysql_insert(table).values(**values)
    stmt = stmt.on_duplicate_key_update(
        source_chat_id=stmt.inserted.source_chat_id,
        source_user_name=stmt.inserted.source_user_name,
        start_time=stmt.inserted.start_time,
        end_time=stmt.inserted.end_time,
        duration_minutes=stmt.inserted.duration_minutes,
        raw_start_text=stmt.inserted.raw_start_text,
        raw_end_text=stmt.inserted.raw_end_text,
        updated_at=stmt.inserted.updated_at,
    )
    engine = _get_engine()
    with engine.begin() as conn:
        conn.execute(stmt)
