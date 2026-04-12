"""Test attachment operations.

Tests both the new sub-resource API and legacy backward-compatible endpoints.
"""

import os
import tempfile
import uuid
from types import SimpleNamespace

import pytest
import requests as http_requests
from tofupilot.v2 import TofuPilot
from tofupilot.v2.errors import ErrorNOTFOUND
from ..utils import get_random_test_dates


class TestLegacyInitialize:
    """Test legacy POST /v2/attachments (initialize) - backward compat."""

    def test_initialize_returns_upload_url(self, client: TofuPilot) -> None:
        result = client.attachments.initialize(name="test.txt")
        assert result.id
        assert result.upload_url
        assert "http" in result.upload_url


class TestLegacyLifecycle:
    """Test legacy initialize -> upload -> finalize flow - backward compat."""

    def test_full_lifecycle(self, client: TofuPilot) -> None:
        init = client.attachments.initialize(name="lifecycle_test.txt")

        resp = http_requests.put(
            init.upload_url,
            data=b"lifecycle test content",
            headers={"Content-Type": "text/plain"},
        )
        assert resp.status_code == 200

        result = client.attachments.finalize(id=init.id)
        assert result.url

    def test_finalize_nonexistent_id(self, client: TofuPilot) -> None:
        with pytest.raises(ErrorNOTFOUND):
            client.attachments.finalize(id="00000000-0000-0000-0000-000000000000")


class TestRunAttachmentsUpload:
    """Test runs.attachments.upload() helper."""

    def test_upload_to_run(self, client: TofuPilot, procedure_id: str) -> None:
        started_at, ended_at = get_random_test_dates()
        run = client.runs.create(
            serial_number=f"UP-{uuid.uuid4().hex[:8]}",
            procedure_id=procedure_id,
            part_number="TEST-PCB-001",
            started_at=started_at,
            ended_at=ended_at,
            outcome="PASS",
        )

        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
            f.write(b"upload helper test")
            f.flush()
            path = f.name

        try:
            attachment_id = client.runs.attachments.upload(id=run.id, file=path)
            assert attachment_id
            assert len(attachment_id) == 36

            fetched = client.runs.get(id=run.id)
            assert fetched.attachments
            assert any(a.id == attachment_id for a in fetched.attachments)
        finally:
            os.unlink(path)


class TestRunAttachmentsDownload:
    """Test runs.attachments.download() helper."""

    def test_download_uploaded_file(self, client: TofuPilot, procedure_id: str) -> None:
        content = b"round-trip download test"

        started_at, ended_at = get_random_test_dates()
        run = client.runs.create(
            serial_number=f"DL-{uuid.uuid4().hex[:8]}",
            procedure_id=procedure_id,
            part_number="TEST-PCB-001",
            started_at=started_at,
            ended_at=ended_at,
            outcome="PASS",
        )

        result = client.runs.create_attachment(id=run.id, name="download_test.txt")
        http_requests.put(result.upload_url, data=content, headers={"Content-Type": "text/plain"})

        fetched = client.runs.get(id=run.id)
        assert fetched.attachments
        attached = next((a for a in fetched.attachments if a.id == result.id), None)
        assert attached is not None
        assert attached.download_url

        attachment = SimpleNamespace(name="download_test.txt", download_url=attached.download_url)

        dest = tempfile.mktemp(suffix=".txt")
        try:
            path = client.runs.attachments.download(attachment, dest=dest)
            assert path.exists()
            assert path.read_bytes() == content
        finally:
            if os.path.exists(dest):
                os.unlink(dest)

    def test_download_no_url(self, client: TofuPilot) -> None:
        attachment = SimpleNamespace(name="missing.txt", download_url=None)
        with pytest.raises(ValueError, match="no download URL"):
            client.runs.attachments.download(attachment)


class TestNewRunEndpoint:
    """Test POST /v2/runs/{id}/attachments raw endpoint."""

    def test_create_attachment_returns_id_and_url(self, client: TofuPilot, procedure_id: str) -> None:
        started_at, ended_at = get_random_test_dates()
        run = client.runs.create(
            serial_number=f"RAW-{uuid.uuid4().hex[:8]}",
            procedure_id=procedure_id,
            part_number="TEST-PCB-001",
            started_at=started_at,
            ended_at=ended_at,
            outcome="PASS",
        )

        result = client.runs.create_attachment(id=run.id, name="raw_test.pdf")
        assert result.id
        assert len(result.id) == 36
        assert result.upload_url
        assert "http" in result.upload_url


class TestNewUnitEndpoint:
    """Test POST /v2/units/{serial}/attachments raw endpoint."""

    def test_create_attachment_returns_id_and_url(self, client: TofuPilot, create_test_unit) -> None:
        _, serial, _ = create_test_unit("RAWUNIT")

        result = client.units.create_attachment(serial_number=serial, name="raw_unit_test.pdf")
        assert result.id
        assert len(result.id) == 36
        assert result.upload_url
        assert "http" in result.upload_url
