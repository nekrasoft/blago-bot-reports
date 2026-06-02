from __future__ import annotations

from datetime import date


def get_month_range(today: date, month_offset: int = 0) -> tuple[date, date]:
    month_index = today.year * 12 + today.month - 1 + month_offset
    year, zero_based_month = divmod(month_index, 12)
    month = zero_based_month + 1
    start = date(year, month, 1)

    next_year, next_zero_based_month = divmod(month_index + 1, 12)
    end = date(next_year, next_zero_based_month + 1, 1)
    return start, end
