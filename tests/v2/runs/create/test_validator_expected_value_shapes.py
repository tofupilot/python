"""Validator expected values a test framework can legitimately send.

A JSON measurement compared against a reference object (`== {}` for "no
deviation from the golden configuration") and a value checked against a
mixed list used to fail the whole run with 400 "Input validation failed":
the run stayed in the station's upload queue and its results never
reached the dashboard. They are now accepted and stored as the
validator's expression with the outcome the framework computed.
"""

from tofupilot.v2 import TofuPilot
from ...utils import get_random_test_dates, assert_create_run_success
from ....e2e_tag import uid


def _run_with(client: TofuPilot, procedure_id: str, measurement: dict):
    started_at, ended_at = get_random_test_dates()
    return client.runs.create(
        serial_number=f"SN-EXPVAL-{uid()}",
        procedure_id=procedure_id,
        part_number="TEST-EXPVAL-001",
        started_at=started_at,
        ended_at=ended_at,
        outcome="PASS",
        phases=[
            {
                "name": "Expected value shapes",
                "outcome": "PASS",
                "started_at": started_at,
                "ended_at": ended_at,
                "measurements": [measurement],
            }
        ],
    )


def test_object_expected_value_on_json_measurement(client: TofuPilot, procedure_id: str):
    result = _run_with(
        client,
        procedure_id,
        {
            "name": "golden_config_mismatches",
            "outcome": "PASS",
            "measured_value": {},
            "validators": [{"operator": "==", "expected_value": {}, "outcome": "PASS"}],
        },
    )
    assert_create_run_success(result)


def test_nested_object_expected_value(client: TofuPilot, procedure_id: str):
    reference = {"SERVO1_FUNCTION": 33, "frame": "quad+"}
    result = _run_with(
        client,
        procedure_id,
        {
            "name": "autopilot_config",
            "outcome": "PASS",
            "measured_value": reference,
            "validators": [{"operator": "==", "expected_value": reference, "outcome": "PASS"}],
        },
    )
    assert_create_run_success(result)


def test_mixed_list_expected_value(client: TofuPilot, procedure_id: str):
    result = _run_with(
        client,
        procedure_id,
        {
            "name": "spinning_output",
            "outcome": "PASS",
            "measured_value": "M3",
            "validators": [{"operator": "in", "expected_value": ["M3", 1], "outcome": "PASS"}],
        },
    )
    assert_create_run_success(result)
