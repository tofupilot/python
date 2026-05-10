"""Test custom metadata on runs (and unit_metadata at run-create time)."""

import uuid
from typing import Optional, Tuple
import pytest
from tofupilot.v2 import TofuPilot
from tofupilot.v2.errors import APIError, ErrorBADREQUEST
from ...utils import get_random_test_dates


@pytest.fixture(scope="class")
def metadata_procedure_id(_v1_test_procedure) -> str:
    """Reuse the session-scoped V2-created procedure that auto-links the
    test station so station-auth tests can hit it directly.
    """
    return _v1_test_procedure["id"]


def _create_run(
    client: TofuPilot,
    procedure_id: str,
    metadata: Optional[dict] = None,
    unit_metadata: Optional[dict] = None,
) -> Tuple[str, str]:
    """Create a run with a fresh serial. Returns (run_id, serial_number)."""
    serial = f"META-RUN-{uuid.uuid4().hex[:8]}"
    started_at, ended_at = get_random_test_dates()
    kwargs = dict(
        serial_number=serial,
        procedure_id=procedure_id,
        part_number="META-RUN-PART",
        outcome="PASS",
        started_at=started_at,
        ended_at=ended_at,
    )
    if metadata is not None:
        kwargs["metadata"] = metadata
    if unit_metadata is not None:
        kwargs["unit_metadata"] = unit_metadata
    res = client.runs.create(**kwargs)
    return res.id, serial


class TestRunMetadata:

    def test_create_run_with_metadata(
        self, client: TofuPilot, metadata_procedure_id: str
    ) -> None:
        """Metadata round-trips with native types."""
        run_id, _ = _create_run(
            client,
            metadata_procedure_id,
            metadata={
                "supply_voltage_v": 12.0,
                "load_resistor_ohm": 50,
                "chamber_id": "CHAMBER-2",
                "is_retest": False,
            },
        )
        run = client.runs.get(id=run_id)
        assert run.metadata is not None
        assert run.metadata["supply_voltage_v"] == 12.0
        assert run.metadata["load_resistor_ohm"] == 50
        assert run.metadata["chamber_id"] == "CHAMBER-2"
        assert run.metadata["is_retest"] is False

    def test_create_run_with_unit_metadata(
        self, client: TofuPilot, metadata_procedure_id: str
    ) -> None:
        """unit_metadata in runs.create stamps onto the unit under test."""
        run_id, serial = _create_run(
            client,
            metadata_procedure_id,
            unit_metadata={"wifi_chipset": "ESP32-S3"},
        )
        unit = client.units.get(serial_number=serial)
        assert unit.metadata is not None
        assert unit.metadata["wifi_chipset"] == "ESP32-S3"

    def test_update_run_metadata_patch(
        self, client: TofuPilot, metadata_procedure_id: str
    ) -> None:
        """PATCH semantics on the run metadata endpoint."""
        run_id, _ = _create_run(
            client,
            metadata_procedure_id,
            metadata={"a": "first", "b": "second"},
        )
        client.runs.update_metadata(id=run_id, metadata={"a": "updated"})
        run = client.runs.get(id=run_id)
        assert run.metadata == {"a": "updated", "b": "second"}

    def test_update_run_metadata_null_deletes(
        self, client: TofuPilot, metadata_procedure_id: str
    ) -> None:
        """null value on the run update path deletes the key."""
        run_id, _ = _create_run(
            client, metadata_procedure_id, metadata={"keep": "x", "drop": "y"}
        )
        client.runs.update_metadata(id=run_id, metadata={"drop": None})
        run = client.runs.get(id=run_id)
        assert run.metadata == {"keep": "x"}

    def test_update_run_metadata_null_diff_replaces(
        self, client: TofuPilot, metadata_procedure_id: str
    ) -> None:
        """Caller mimics 'replace' by sending null for keys to drop."""
        run_id, _ = _create_run(
            client, metadata_procedure_id, metadata={"a": 1, "b": 2}
        )
        client.runs.update_metadata(
            id=run_id, metadata={"only": "this", "a": None, "b": None}
        )
        run = client.runs.get(id=run_id)
        assert run.metadata == {"only": "this"}

    def test_run_metadata_invalid_key_rejected(
        self, client: TofuPilot, metadata_procedure_id: str
    ) -> None:
        """Invalid key chars rejected at the API boundary."""
        run_id, _ = _create_run(client, metadata_procedure_id)
        with pytest.raises((ErrorBADREQUEST, APIError)):
            client.runs.update_metadata(
                id=run_id, metadata={"bad key!": "x"}
            )

    def test_run_metadata_max_50_keys(
        self, client: TofuPilot, metadata_procedure_id: str
    ) -> None:
        """50-key cap is enforced pre-apply."""
        run_id, _ = _create_run(client, metadata_procedure_id)
        too_many = {f"k{i}": i for i in range(51)}
        with pytest.raises((ErrorBADREQUEST, APIError)):
            client.runs.update_metadata(id=run_id, metadata=too_many)
        run = client.runs.get(id=run_id)
        assert not run.metadata

    @pytest.mark.skip(
        reason="Query param encoding mismatch: SDK form-encodes nested dict as "
        "metadata[k][in]=v but server expects JSON-encoded string. Filter works "
        "via internal caller; needs wider tRPC-rest query serialization fix."
    )
    def test_list_runs_metadata_filter(
        self, client: TofuPilot, metadata_procedure_id: str
    ) -> None:
        """list with metadata filter returns only matching runs."""
        marker = f"fix-{uuid.uuid4().hex[:6]}"
        run_match, _ = _create_run(
            client, metadata_procedure_id, metadata={"fixture": marker}
        )
        run_other, _ = _create_run(
            client, metadata_procedure_id, metadata={"fixture": "other"}
        )
        result = client.runs.list(
            metadata={"fixture": {"in": [marker]}},
            include_metadata=True,
        )
        ids = [r.id for r in result.data]
        assert run_match in ids
        assert run_other not in ids

    def test_run_get_activity_records_metadata_changes(
        self, client: TofuPilot, metadata_procedure_id: str
    ) -> None:
        """Activity log records set + change + remove actions."""
        run_id, _ = _create_run(client, metadata_procedure_id, metadata={"k": "v1"})
        client.runs.update_metadata(id=run_id, metadata={"k": "v2"})
        client.runs.update_metadata(id=run_id, metadata={"k": None})

        # getActivity is a web-router endpoint, not part of V2 SDK; reach
        # it via the underlying tRPC URL only if exposed. Skip if missing.
        if not hasattr(client.runs, "get_activity"):
            pytest.skip("runs.get_activity not exposed in this SDK build")
        activity = client.runs.get_activity(id=run_id)
        actions = [c.action for c in activity.metadata_changes]
        assert "created" in actions
        assert "updated" in actions
        assert "removed" in actions
