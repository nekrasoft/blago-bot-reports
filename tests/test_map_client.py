from __future__ import annotations

import unittest

from map_client import _address_to_note, format_note_with_bunker_numbers


class AddressToNoteTest(unittest.TestCase):
    def test_keeps_address_without_city(self) -> None:
        note = _address_to_note("Тихая, 12")

        self.assertEqual(
            format_note_with_bunker_numbers(note, [7]),
            "Тихая, 12 # 7",
        )

    def test_removes_kirov_prefix(self) -> None:
        self.assertEqual(_address_to_note("Киров, Тихая, 12"), "Тихая, 12")


if __name__ == "__main__":
    unittest.main()
