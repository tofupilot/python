"""Config and secrets: recorded on create, returned on get.

A run records the config values it used with their values, and the secrets it used by
the stored value and version they came from, never by value. Users and
stations can both send them (the `client` fixture runs each test as both).
"""

import uuid

import pytest
from tofupilot.v2 import TofuPilot
from tofupilot.v2.errors import ErrorBADREQUEST
from ...utils import get_random_test_dates, assert_create_run_success
from ....e2e_tag import uid


def _run(procedure_id: str, unique: str) -> dict:
    started_at, ended_at = get_random_test_dates()
    return {
        "serial_number": f"SN-VALUES-{unique}",
        "procedure_id": procedure_id,
        "part_number": f"PART-VALUES-{unique}",
        "started_at": started_at,
        "ended_at": ended_at,
        "outcome": "PASS",
    }


def test_config_and_secrets_round_trip(client: TofuPilot, procedure_id: str):
    """Each var comes back in its declared type; a secret comes back without a value."""
    unique = uid()
    var_id = str(uuid.uuid4())
    secret_id = str(uuid.uuid4())

    result = client.runs.create(
        **_run(procedure_id, unique),
        config=[
            {"key": "DMM_RESOURCE", "value": "TCPIP0::192.168.1.20::INSTR", "variable_id": var_id, "version": 3, "source": "procedure"},
            {"key": "OFFSET_MV", "value": 1.5, "source": "dotenv"},
            {"key": "DRY_RUN", "value": False, "source": "default"},
        ],
        secrets=[
            {"key": "MES_TOKEN", "variable_id": secret_id, "version": 1, "source": "station"},
        ],
    )
    assert_create_run_success(result)

    run = client.runs.get(id=result.id)

    by_key = {v.key: v for v in run.config}
    assert set(by_key) == {"DMM_RESOURCE", "OFFSET_MV", "DRY_RUN"}
    assert by_key["DMM_RESOURCE"].value == "TCPIP0::192.168.1.20::INSTR"
    assert by_key["DMM_RESOURCE"].variable_id == var_id
    assert by_key["DMM_RESOURCE"].version == 3
    assert by_key["DMM_RESOURCE"].source == "procedure"
    assert by_key["OFFSET_MV"].value == 1.5
    assert by_key["OFFSET_MV"].variable_id is None
    assert by_key["OFFSET_MV"].version is None
    # bool before int: True == 1 would hide a value read back as a number
    assert by_key["DRY_RUN"].value is False

    assert len(run.secrets) == 1
    secret = run.secrets[0]
    assert secret.key == "MES_TOKEN"
    assert secret.variable_id == secret_id
    assert secret.version == 1
    assert secret.source == "station"
    assert "value" not in secret.model_dump()


def test_a_run_without_values_returns_empty_lists(client: TofuPilot, procedure_id: str):
    """Both fields are always present on get, empty when the run recorded none."""
    result = client.runs.create(**_run(procedure_id, uid()))
    assert_create_run_success(result)

    run = client.runs.get(id=result.id)

    assert run.config == []
    assert run.secrets == []


@pytest.mark.parametrize("field", ["config", "secrets"])
def test_a_duplicate_key_is_rejected(client: TofuPilot, procedure_id: str, field: str):
    """One key per list: the run would not say which value it used."""
    entry = {"key": "DUP", "value": "a"} if field == "config" else {"key": "DUP"}

    with pytest.raises(ErrorBADREQUEST):
        client.runs.create(**_run(procedure_id, uid()), **{field: [entry, dict(entry)]})
