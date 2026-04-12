"""Test attaching files to units via the v2 API.

Tests the sub-resource workflow:
1. client.units.attachments.upload(serial_number, file) - attach
2. client.units.attachments.delete(serial_number, ids) - delete
"""

import os
import tempfile

from tofupilot.v2 import TofuPilot
from ..utils import assert_get_unit_success


class TestUnitAttachments:
    """Test the unit attachment workflow."""

    def test_attach_file_to_unit(self, client: TofuPilot, auth_type: str, create_test_unit, timestamp) -> None:
        """Attach a file to a unit using the sub-resource helper."""
        _, serial, _ = create_test_unit("ATTACH")

        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
            f.write(b"unit attachment test content")
            f.flush()
            path = f.name

        try:
            attachment_id = client.units.attachments.upload(serial_number=serial, file=path)
            assert attachment_id
            assert len(attachment_id) == 36

            unit = client.units.get(serial_number=serial)
            assert_get_unit_success(unit)
            assert unit.attachments is not None
            assert len(unit.attachments) >= 1
            attached = next((a for a in unit.attachments if a.id == attachment_id), None)
            assert attached is not None
        finally:
            os.unlink(path)

    def test_delete_attachment_from_unit(self, client: TofuPilot, auth_type: str, create_test_unit, timestamp) -> None:
        """Attach a file, then delete it via attachments.delete()."""
        _, serial, _ = create_test_unit("DELATT")

        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
            f.write(b"file to delete")
            f.flush()
            path = f.name

        try:
            attachment_id = client.units.attachments.upload(serial_number=serial, file=path)
            assert attachment_id

            result = client.units.attachments.delete(serial_number=serial, ids=[attachment_id])
            assert attachment_id in result.ids

            unit = client.units.get(serial_number=serial)
            remaining = [a for a in (unit.attachments or []) if a.id == attachment_id]
            assert len(remaining) == 0
        finally:
            os.unlink(path)
