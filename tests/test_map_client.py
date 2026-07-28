from __future__ import annotations

import unittest

from map_client import (
    _address_to_note,
    dedupe_bunker_items,
    format_note_with_bunker_numbers,
    get_bunker_identity_key,
)


class AddressToNoteTest(unittest.TestCase):
    def test_keeps_address_without_city(self) -> None:
        note = _address_to_note("Тихая, 12")

        self.assertEqual(
            format_note_with_bunker_numbers(note, [7]),
            "Тихая, 12 # 7",
        )

    def test_removes_kirov_prefix(self) -> None:
        self.assertEqual(_address_to_note("Киров, Тихая, 12"), "Тихая, 12")

    def test_bunker_identity_ignores_api_id(self) -> None:
        first = {
            "id": "map-id-1",
            "contractor": "Алтай-Строй",
            "number": "16",
            "address": "Киров, Хлебозаводской проезд, 30",
        }
        second = {
            "id": "map-id-2",
            "contractor": " алтай-строй ",
            "number": "№16",
            "address": "Хлебозаводской проезд, 30",
        }

        self.assertEqual(
            get_bunker_identity_key(first),
            get_bunker_identity_key(second),
        )

    def test_dedupe_bunker_items_keeps_first_physical_bunker(self) -> None:
        items = [
            {
                "id": "map-id-1",
                "contractor": "Алтай-Строй",
                "number": "4",
                "address": "Киров, Хлебозаводской проезд, 30",
            },
            {
                "id": "map-id-2",
                "contractor": "Алтай-Строй",
                "number": "4",
                "address": "Хлебозаводской проезд, 30",
            },
            {
                "id": "map-id-3",
                "contractor": "Алтай-Строй",
                "number": "16",
                "address": "Киров, Хлебозаводской проезд, 30",
            },
        ]

        self.assertEqual(dedupe_bunker_items(items), [items[0], items[2]])


if __name__ == "__main__":
    unittest.main()
