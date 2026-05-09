from __future__ import annotations

import re
import uuid

WAYBILL_TOKEN_RE = re.compile(
    r"(?:^|\s)\[?ПЛ:(?P<token>[A-Za-z0-9_-]{8,64})\]?",
    re.IGNORECASE,
)


def generate_waybill_token() -> str:
    return f"wb_{uuid.uuid4().hex[:16]}"


def format_note_with_waybill_token(note: str | None, token: str | None) -> str:
    marker = f"ПЛ:{str(token or '').strip()}"
    if marker == "ПЛ:":
        return str(note or "").strip()

    note_text = str(note or "").strip()
    if not note_text:
        return marker
    return f"{note_text} [{marker}]"


def extract_waybill_token(note: str | None) -> tuple[str, str | None]:
    raw = str(note or "").strip()
    if not raw:
        return "", None

    match = WAYBILL_TOKEN_RE.search(raw)
    if not match:
        return raw, None

    clean_note = (raw[: match.start()] + raw[match.end() :]).strip()
    clean_note = re.sub(r"\s{2,}", " ", clean_note)
    clean_note = clean_note.strip(" ;,")
    return clean_note, match.group("token")
