from __future__ import annotations

import unittest
from datetime import date

from driver_work_time_periods import format_month, get_month_range


class DriverWorkTimePeriodsTest(unittest.TestCase):
    def test_current_month_range(self) -> None:
        self.assertEqual(
            get_month_range(date(2026, 6, 2)),
            (date(2026, 6, 1), date(2026, 7, 1)),
        )

    def test_previous_month_range(self) -> None:
        self.assertEqual(
            get_month_range(date(2026, 6, 2), month_offset=-1),
            (date(2026, 5, 1), date(2026, 6, 1)),
        )

    def test_previous_month_range_crosses_year_boundary(self) -> None:
        self.assertEqual(
            get_month_range(date(2026, 1, 15), month_offset=-1),
            (date(2025, 12, 1), date(2026, 1, 1)),
        )

    def test_format_month(self) -> None:
        self.assertEqual(format_month(date(2026, 5, 1)), "05.2026")


if __name__ == "__main__":
    unittest.main()
