from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from hashlib import sha256
from uuid import uuid4

from bunker_pickup import build_pickup_sheet_row
from map_client import create_pickup_report
from waybill_files_db import save_waybill_file


def submission_identity(data: dict) -> str:
    return data.setdefault("submission_key", str(uuid4()))


def prepare_submission(data: dict, *, source: str, user_id, name, chat_id, message_id=None) -> dict:
    if data.get("submission"):
        return data["submission"]
    key = submission_identity(data)
    log = deepcopy(data["bunker_log"])
    photos = deepcopy(data.get("site_photos") or [])
    waybills = deepcopy(data.get("waybills") or [])
    tokens = [sha256(f"{key}:waybill:{index}".encode()).hexdigest() for index in range(len(waybills))]
    submission = {
        "request": dict(submission_key=key,
            items=[{"bunkerId": item["id"], "billingUnits": item["billing_units"]} for item in log],
            cleanup_status=data.get("cleanup_status") or "not_required", cleanup_comment=data.get("cleanup_comment"),
            driver_source=source, driver_user_id=user_id, driver_name=name, site_photos=photos,
            waybills=waybills, waybill_tokens=tokens, waybill_missing_reason=data.get("waybill_missing_reason")),
        "row": build_pickup_sheet_row(log, datetime.now().strftime("%d.%m.%Y"), tokens),
        "chat_id": chat_id, "message_id": message_id,
    }
    data["submission"] = submission
    return submission


def send_submission(submission: dict) -> dict:
    request = submission["request"]
    for file, token in zip(request["waybills"], request["waybill_tokens"], strict=True):
        save_waybill_file(file_token=token, source=request["driver_source"], file_bytes=file["file_bytes"],
            source_chat_id=submission["chat_id"], source_user_id=request["driver_user_id"],
            source_message_id=file.get("source_message_id") or submission["message_id"],
            source_file_id=file.get("source_file_id"), file_name=file.get("file_name"), content_type=file.get("content_type"))
    return create_pickup_report(**request, sheet_row=submission["row"])
