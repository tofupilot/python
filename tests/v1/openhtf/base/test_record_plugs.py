"""record_plugs: an OpenHTF run records the instrument behind each plug.

OpenHTF's JSON record keeps no plugs; record_plugs reads them in a teardown
phase while they are still open, and the run gets them as its `plugs`.
"""

import uuid

import openhtf as htf
from openhtf.plugs import BasePlug

from tofupilot.openhtf import record_plugs, upload


class PowerSupply(BasePlug):
    def _asdict(self):
        return {"manufacturer": "RIGOL TECHNOLOGIES", "model": "DP832", "serial_number": "DP8B241200417",
                "firmware": "00.01.16", "voltage": 24.0}


class Daq(BasePlug):
    def _asdict(self):
        return {"model": "DAQ-8", "firmware": 1.0}


@htf.plug(daq=Daq)
def read_daq(test, daq):
    pass


class Relays(BasePlug):
    """Reports nothing about itself: recorded without identity."""


class BrokenState(BasePlug):
    def _asdict(self):
        raise OSError("instrument gone")


@htf.plug(psu=PowerSupply)
def power_up(test, psu):
    pass


@htf.measures(htf.Measurement("rail").in_range(3.2, 3.4))
@htf.plug(psu=PowerSupply, relays=Relays, broken=BrokenState)
def measure_rail(test, psu, relays, broken):
    test.measurements.rail = 3.3


def failing_phase(test):
    return htf.PhaseResult.STOP


def _run(tofupilot_server_url, api_key, procedure_identifier, *phases):
    serial = f"PLUGS-{uuid.uuid4().hex[:8]}"
    test = htf.Test(record_plugs(*phases), procedure_id=procedure_identifier, part_number="test_record_plugs")
    test.add_output_callbacks(upload(url=tofupilot_server_url, api_key=api_key))
    test.execute(lambda: serial)
    return serial


def test_plugs_are_recorded_with_their_identity(
    tofupilot_server_url, api_key, procedure_identifier, procedure_id, extract_id_and_check_run_exists,
):
    serial = _run(tofupilot_server_url, api_key, procedure_identifier, power_up, measure_rail)
    run = extract_id_and_check_run_exists(serial_number=serial, procedure_id=procedure_id,
                                          part_number="test_record_plugs")

    plugs = {p.name: p for p in run.plugs}
    assert set(plugs) == {"PowerSupply", "Relays", "BrokenState"}
    psu = plugs["PowerSupply"]
    assert (psu.manufacturer, psu.model, psu.serial_number, psu.firmware) == (
        "RIGOL TECHNOLOGIES", "DP832", "DP8B241200417", "00.01.16")
    assert plugs["Relays"].serial_number is None
    assert plugs["BrokenState"].serial_number is None
    # The phase that collects them is not a test step.
    assert [p.name for p in run.phases if p.name.startswith("tofupilot")] == []


def test_plugs_are_recorded_when_the_test_stops_early(
    tofupilot_server_url, api_key, procedure_identifier, procedure_id, extract_id_and_check_run_exists,
):
    serial = _run(tofupilot_server_url, api_key, procedure_identifier, power_up, failing_phase, measure_rail)
    run = extract_id_and_check_run_exists(serial_number=serial, procedure_id=procedure_id,
                                          part_number="test_record_plugs")
    assert "PowerSupply" in {p.name for p in run.plugs}


def test_the_record_ties_each_phase_to_the_plugs_it_received():
    """What upload() sends: the server turns phase_plugs into each phase's plug_keys."""
    from openhtf.output.callbacks import json_factory
    import json

    records = []
    test = htf.Test(record_plugs(power_up, read_daq, failing_phase, measure_rail))
    test.add_output_callbacks(
        lambda record: records.append(json.loads("".join(json_factory.OutputToJSON("/dev/null").serialize_test_record(record))))
    )
    test.execute(lambda: f"PLUGS-{uuid.uuid4().hex[:8]}")

    path = f"{__name__}.PowerSupply"
    recorded = records[0]["metadata"]["tofupilot_plugs"]
    assert recorded["phase_plugs"] == {
        "power_up": [path],
        "read_daq": [f"{__name__}.Daq"],
        "failing_phase": [],
        "measure_rail": [path, f"{__name__}.Relays", f"{__name__}.BrokenState"],
    }
    assert recorded["plug_states"][path]["serial_number"] == "DP8B241200417"
    assert "voltage" not in recorded["plug_states"][path]
    assert recorded["plug_states"][f"{__name__}.BrokenState"] == {}
    # A number stays as written: "1.0", not 1.
    assert recorded["plug_states"][f"{__name__}.Daq"]["firmware"] == "1.0"
    # measure_rail never ran (the test stopped), its plugs were still read.
    assert [p["name"] for p in records[0]["phases"]][-1] == "tofupilot_record_plugs"


def test_a_placeholder_plug_asks_for_with_plugs_first():
    import pytest
    from openhtf.core.base_plugs import PlugPlaceholder

    @htf.plug(psu=PlugPlaceholder(PowerSupply))
    def uses_placeholder(test, psu):
        pass

    with pytest.raises(ValueError, match="with_plugs"):
        record_plugs(uses_placeholder)
    # Resolved before wrapping: fine.
    record_plugs(uses_placeholder.with_plugs(psu=PowerSupply))
