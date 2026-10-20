from __future__ import annotations

import hashlib
import unittest
from unittest.mock import MagicMock, patch

from sqlalchemy.dialects import mysql

from waybill_files_db import save_waybill_file


class WaybillFilesDbTest(unittest.TestCase):
    def test_save_compiles_mysql_upsert_for_waybill_and_photo(self):
        for kind in ("container_waybill", "site_photo"):
            with self.subTest(kind=kind):
                engine = MagicMock()
                with patch("waybill_files_db._get_engine", return_value=engine):
                    digest = save_waybill_file(
                        file_token="test-token", source="max", file_bytes=b"file", kind=kind,
                    )
                connection = engine.begin.return_value.__enter__.return_value
                connection.execute.assert_called_once()
                statement = connection.execute.call_args.args[0]
                compiled = statement.compile(dialect=mysql.dialect())
                self.assertEqual(compiled.params["kind"], kind)
                self.assertEqual(compiled.params["file_data"], b"file")
                self.assertIn("kind = VALUES(kind)", str(compiled))
                self.assertEqual(digest, hashlib.sha256(b"file").hexdigest())
