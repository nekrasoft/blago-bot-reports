from __future__ import annotations

import unittest
from datetime import datetime, timezone

from driver_time_buttons import (
    DRIVER_START_TIME_OPTIONS,
    get_driver_end_time_options,
    get_driver_time_buttons,
    to_moscow_time,
)


class DriverTimeButtonsTest(unittest.TestCase):
    def test_start_time_buttons(self) -> None:
        self.assertEqual(
            get_driver_time_buttons(DRIVER_START_TIME_OPTIONS, "vstart"),
            [
                ("08:30", "vstart:08:30"),
                ("09:00", "vstart:09:00"),
                ("09:30", "vstart:09:30"),
            ],
        )

    def test_end_time_buttons_are_based_on_command_time(self) -> None:
        options = get_driver_end_time_options(datetime(2026, 6, 2, 20, 43))

        self.assertEqual(options, ["20:20", "20:30", "20:40"])
        self.assertEqual(
            get_driver_time_buttons(options, "vend"),
            [
                ("20:20", "vend:20:20"),
                ("20:30", "vend:20:30"),
                ("20:40", "vend:20:40"),
            ],
        )

    def test_end_time_options_round_down_to_ten_minutes(self) -> None:
        self.assertEqual(
            get_driver_end_time_options(datetime(2026, 6, 2, 9, 0)),
            ["08:40", "08:50", "09:00"],
        )

    def test_end_time_options_support_midnight_boundary(self) -> None:
        self.assertEqual(
            get_driver_end_time_options(datetime(2026, 6, 2, 0, 3)),
            ["23:40", "23:50", "00:00"],
        )

    def test_moscow_time_is_three_hours_ahead_of_utc(self) -> None:
        moscow_time = to_moscow_time(
            datetime(2026, 6, 2, 17, 43, tzinfo=timezone.utc)
        )

        self.assertEqual(
            moscow_time,
            datetime.fromisoformat("2026-06-02T20:43:00+03:00"),
        )
        self.assertEqual(
            get_driver_end_time_options(moscow_time),
            ["20:20", "20:30", "20:40"],
        )

    def test_moscow_time_rejects_naive_datetime(self) -> None:
        with self.assertRaises(ValueError):
            to_moscow_time(datetime(2026, 6, 2, 17, 43))


if __name__ == "__main__":
    unittest.main()
