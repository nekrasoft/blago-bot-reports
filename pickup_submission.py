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


def prepare_submission(data: dict, *, source: str, user_id, name, chat_id, message_id=None, trip_details=None, sheet_row=None) -> dict:
    if data.get("submission"):
        return data["submission"]
    key = submission_identity(data)
    log = deepcopy(data.get("bunker_log") or [])
    photos = deepcopy(data.get("site_photos") or [])
    waybills = deepcopy(data.get("waybills") or [])
    tokens = [sha256(f"{key}:waybill:{index}".encode()).hexdigest() for index in range(len(waybills))]
    photo_tokens = [sha256(f"{key}:photo:{index}".encode()).hexdigest() for index in range(len(photos))]
    submission = {
        "request": dict(submission_key=key,
            items=[{"bunkerId": item["id"], "billingUnits": item["billing_units"]} for item in log],
            cleanup_status=data.get("cleanup_status") or "not_required", cleanup_comment=data.get("cleanup_comment"),
            driver_source=source, driver_user_id=user_id, driver_name=name, site_photos=photos,
            waybills=waybills, waybill_tokens=tokens, waybill_missing_reason=data.get("waybill_missing_reason")),
        "row": build_pickup_sheet_row(log, datetime.now().strftime("%d.%m.%Y"), tokens + photo_tokens) if trip_details is None else sheet_row,
        "photo_tokens": photo_tokens, "chat_id": chat_id, "message_id": message_id,
    }
    if trip_details is not None:
        submission["request"].update(report_type="trip", trip_details=deepcopy(trip_details))
        submission["row"] = deepcopy(sheet_row)
    data["submission"] = submission
    return submission


def send_submission(submission: dict) -> dict:
    request = submission["request"]
    for file, token in zip(request["waybills"] + request["site_photos"],
                           request["waybill_tokens"] + submission["photo_tokens"], strict=True):
        save_waybill_file(file_token=token, source=request["driver_source"], file_bytes=file["file_bytes"],
            kind="site_photo" if token in submission["photo_tokens"] else "container_waybill",
            source_chat_id=submission["chat_id"], source_user_id=request["driver_user_id"],
            source_message_id=file.get("source_message_id") or submission["message_id"],
            source_file_id=file.get("source_file_id"), file_name=file.get("file_name"), content_type=file.get("content_type"))
    return create_pickup_report(**request, sheet_row=submission["row"])
