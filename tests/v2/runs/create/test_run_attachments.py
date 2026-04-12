"""Test attaching files to runs via the v2 API.

Tests the simplified workflow: client.runs.attach(id, file)
"""

import os
import tempfile
import uuid

from tofupilot.v2 import TofuPilot
from ...utils import get_random_test_dates


class TestRunAttachments:
    """Test the run attachment workflow."""

    def test_attach_file_to_run(self, client: TofuPilot, procedure_id: str) -> None:
        """Attach a file to a run using the simplified attach helper."""
        started_at, ended_at = get_random_test_dates()
        run = client.runs.create(
            serial_number=f"ATTACH-{uuid.uuid4().hex[:8]}",
            procedure_id=procedure_id,
            part_number="TEST-PCB-001",
            started_at=started_at,
            ended_at=ended_at,
            outcome="PASS",
        )

        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
            f.write(b"run attachment test content")
            f.flush()
            path = f.name

        try:
            attachment_id = client.runs.attachments.upload(id=run.id, file=path)
            assert attachment_id
            assert len(attachment_id) == 36

            fetched = client.runs.get(id=run.id)
            assert fetched.attachments is not None
            assert len(fetched.attachments) >= 1
            attached = next((a for a in fetched.attachments if a.id == attachment_id), None)
            assert attached is not None
        finally:
            os.unlink(path)

    def test_attach_nonexistent_file(self, client: TofuPilot) -> None:
        """Attaching a nonexistent file raises FileNotFoundError."""
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
            f.write(b"dummy")
            path = f.name
        os.unlink(path)

        from pytest import raises
        with raises(FileNotFoundError):
            client.runs.attachments.upload(id="00000000-0000-0000-0000-000000000000", file=path)
