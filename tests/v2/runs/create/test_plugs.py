"""A run records the plugs it used and the instrument each one opened."""

from datetime import datetime, timedelta, timezone

import pytest

from tofupilot.v2 import TofuPilot
from tofupilot.v2.errors import APIError, ErrorBADREQUEST
from ...utils import assert_create_run_success
from ....e2e_tag import uid


def _times():
    now = datetime.now(timezone.utc)
    return now - timedelta(minutes=2), now - timedelta(minutes=1)


class TestRunPlugs:
    def test_plugs_round_trip_with_identity(self, client: TofuPilot, procedure_id: str) -> None:
        started, ended = _times()
        serial = f"PSU-{uid()}"
        result = client.runs.create(
            serial_number=f"SN-PLUGS-{uid()}",
            procedure_id=procedure_id,
            part_number=f"PART-PLUGS-{uid()}",
            outcome="PASS",
            started_at=started,
            ended_at=ended,
            plugs=[
                {"key": "psu", "name": "Power Supply", "python": "instruments.psu:Psu", "scope": "station",
                 "manufacturer": " Keysight Technologies ", "model": "E36312A", "serial_number": serial, "firmware": "2.1.3"},
                {"key": "relays", "name": "Relays"},
            ],
            phases=[{"name": "rail", "outcome": "PASS", "started_at": started, "ended_at": ended, "plug_keys": ["psu"],
                     "measurements": [{"name": "voltage", "outcome": "PASS", "measured_value": 3.3, "units": "V"}]}],
            logs=[{"level": "INFO", "timestamp": started, "message": "identified", "source_file": "psu.py",
                   "line_number": 1, "plug_key": "psu"}],
        )
        assert_create_run_success(result)

        run = client.runs.get(id=result.id)
        assert [(p.key, p.manufacturer, p.serial_number, p.firmware, p.scope) for p in run.plugs] == [
            ("psu", "Keysight Technologies", serial, "2.1.3", "station"),
            ("relays", None, None, None, None),
        ]

    def test_a_run_without_plugs_has_an_empty_list(self, client: TofuPilot, procedure_id: str) -> None:
        started, ended = _times()
        result = client.runs.create(
            serial_number=f"SN-NOPLUGS-{uid()}", procedure_id=procedure_id, part_number=f"PART-PLUGS-{uid()}",
            outcome="PASS", started_at=started, ended_at=ended,
        )
        assert client.runs.get(id=result.id).plugs == []

    def test_an_unknown_plug_key_does_not_lose_the_run(self, client: TofuPilot, procedure_id: str) -> None:
        started, ended = _times()
        result = client.runs.create(
            serial_number=f"SN-PLUGKEY-{uid()}", procedure_id=procedure_id, part_number=f"PART-PLUGS-{uid()}",
            outcome="PASS", started_at=started, ended_at=ended,
            plugs=[{"key": "psu", "name": "PSU"}],
            phases=[{"name": "rail", "outcome": "PASS", "started_at": started, "ended_at": ended, "plug_keys": ["dmm"]}],
            logs=[{"level": "INFO", "timestamp": started, "message": "x", "source_file": "a.py", "line_number": 1,
                   "plug_key": "relays"}],
        )
        assert_create_run_success(result)
        assert [p.key for p in client.runs.get(id=result.id).plugs] == ["psu"]

    def test_a_duplicate_key_or_oversized_serial_is_refused(self, client: TofuPilot, procedure_id: str) -> None:
        started, ended = _times()
        base = dict(serial_number=f"SN-PLUGBAD-{uid()}", procedure_id=procedure_id, part_number=f"PART-PLUGS-{uid()}",
                    outcome="PASS", started_at=started, ended_at=ended)
        for plugs in (
            [{"key": "psu", "name": "A"}, {"key": "psu", "name": "B"}],
            [{"key": "psu", "name": "A", "serial_number": "x" * 256}],
        ):
            with pytest.raises((ErrorBADREQUEST, APIError)):
                client.runs.create(**base, plugs=plugs)
