import tempfile
import unittest
from pathlib import Path
from persistent_context import PersistentMaxContext

class PersistentContextTest(unittest.IsolatedAsyncioTestCase):
    async def test_restart_preserves_state_attachments_and_user_isolation(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / 'state.sqlite3'
            first = PersistentMaxContext(1, 2, database=database)
            await first.set_state('BunkerDialog:confirming')
            await first.update_data(photos=[{'file_bytes': b'photo'}], selected_keys={('a', 'b')})
            restored = PersistentMaxContext(1, 2, database=database)
            self.assertEqual(await restored.get_state(), 'BunkerDialog:confirming')
            self.assertEqual((await restored.get_data())['photos'][0]['file_bytes'], b'photo')
            self.assertEqual((await restored.get_data())['selected_keys'], {('a', 'b')})
            self.assertEqual(await PersistentMaxContext(1, 3, database=database).get_data(), {})
            await restored.clear()
            self.assertEqual(await first.get_data(), {})
