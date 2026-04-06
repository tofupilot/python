import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables FIRST, before any other imports
env_path = Path(__file__).parent.parent.parent / ".env.local"
load_dotenv(env_path, override=True)

# Disable posthog during tests, needs to come before `import tofupilot`
os.environ["DISABLE_TELEMETRY"] = "true"

import pytest
import re
import requests
from typing import Union

import tofupilot


@pytest.fixture(scope="class")
def tofupilot_server_url() -> str:
    # Get URL from environment
    url = os.environ.get("TOFUPILOT_URL")

    if not url:
        pytest.fail(
            "TOFUPILOT_URL environment variable not set. "
            "Set it to run tests against local server:\n"
            "export TOFUPILOT_URL='the-url-you-want-to-test'"
        )
    return url


@pytest.fixture(scope="class")
def station_api_key() -> str:
    # Get API key from environment
    api_key = os.environ.get("TOFUPILOT_API_KEY_STATION")

    if not api_key:
        pytest.fail(
            "TOFUPILOT_API_KEY_STATION environment variable not set. "
            "Set it to run tests against local server:\n"
            "export TOFUPILOT_API_KEY_STATION='your-local-api-key'"
        )
    return api_key


@pytest.fixture(scope="class")
def user_api_key() -> str:
    # Get API key from environment
    api_key = os.environ.get("TOFUPILOT_API_KEY_USER")

    if not api_key:
        pytest.fail(
            "TOFUPILOT_API_KEY_USER environment variable not set. "
            "Set it to run tests against local server:\n"
            "export TOFUPILOT_API_KEY_USER='your-local-api-key'"
        )
    return api_key


@pytest.fixture(scope="class", params=["user", "station"])
def api_key(request, user_api_key: str, station_api_key: str) -> str:
    """Fixture that provides both user and station API keys for testing."""
    if request.param == "user":
        return user_api_key
    elif request.param == "station":
        return station_api_key
    else:
        raise ValueError(f"Unknown api_key type: {request.param}")


@pytest.fixture(scope="session")
def _v1_test_procedure():
    """Create a test procedure via V2 API for V1 tests. Session-scoped to avoid recreating."""
    url = os.environ.get("TOFUPILOT_URL")
    api_key = os.environ.get("TOFUPILOT_API_KEY_USER")
    if not url or not api_key:
        pytest.fail("TOFUPILOT_URL and TOFUPILOT_API_KEY_USER must be set")
    v2_client = tofupilot.v2.TofuPilot(
        api_key=api_key,
        server_url=f"{url}/api",
    )
    result = v2_client.procedures.create(name="V1 Test Procedure")
    proc = v2_client.procedures.get(id=result.id)
    # Link test station to the new procedure so station auth tests work
    station_id = os.environ.get("TOFUPILOT_STATION_ID")
    if station_id:
        try:
            requests.post(
                f"{url}/api/trpc/station.linkProcedure",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={"json": {"id": station_id, "procedure_id": proc.id}},
                timeout=10,
            )
        except Exception:
            pass
    return {"id": proc.id, "identifier": proc.identifier or proc.id}


@pytest.fixture(scope="class")
def procedure_identifier(request, _v1_test_procedure) -> str:
    """Procedure identifier for V1 tests — auto-created via V2 API."""
    test_path = str(request.fspath)
    if "/v1/" not in test_path:
        pytest.skip("procedure_identifier fixture is only for v1 tests")
    return _v1_test_procedure["identifier"] or _v1_test_procedure["id"]


@pytest.fixture(scope="class")
def procedure_id(_v1_test_procedure) -> str:
    """Procedure UUID for V1 tests — auto-created via V2 API."""
    return _v1_test_procedure["id"]


@pytest.fixture()
def extract_run_id_from_logs(caplog):
    SUCCESS_LEVEL_NUM = 25

    def extract_id(s: str) -> Union[str, None]:

        res = re.search(r"^Run imported successfully with ID: (.*)$", s)

        return res.group(1) if res else None

    def func() -> str:
        successes = [
            record
            for record in caplog.get_records("call")
            if record.levelno == SUCCESS_LEVEL_NUM
        ]
        ids = [id for record in successes if (id := extract_id(record.getMessage()))]

        assert len(ids) == 1, f"Expected single id, but got: {ids}"

        return ids[0]

    return func


@pytest.fixture()
def check_run_exists(tofupilot_server_url, api_key):
    """Function to check if a run exists, returns the run for further checks"""

    v2_client = tofupilot.v2.TofuPilot(
        api_key=api_key,
        server_url=f"{tofupilot_server_url}/api",
    )

    def func(
        id: str,
        *,
        # Basic run fields
        outcome: Union[str, None] = None,
        # Unit fields
        serial_number: Union[str, None] = None,
        part_number: Union[str, None] = None,
        part_name: Union[str, None] = None,
        revision: Union[str, None] = None,
        batch_number: Union[str, None] = None,
        # Procedure fields
        procedure_id: Union[str, None] = None,
        procedure_name: Union[str, None] = None,
        procedure_version: Union[str, None] = None,
        # Optional fields
        docstring: Union[str, None] = None,
    ):

        run = v2_client.runs.get(id=id)
        assert run.id == id

        # Basic run fields
        if outcome:
            assert run.outcome == outcome

        # Unit fields
        if serial_number:
            assert run.unit.serial_number == serial_number
        if part_number:
            assert run.unit.part.number == part_number
        if part_name:
            assert run.unit.part.name == part_name
        if revision:
            assert run.unit.part.revision.number == revision
        if batch_number:
            assert run.unit.batch
            assert run.unit.batch.number == batch_number

        # Procedure fields
        if procedure_id:
            assert run.procedure.id == procedure_id
        if procedure_name:
            assert run.procedure.name == procedure_name
        if procedure_version:
            assert run.procedure.version
            assert run.procedure.version.tag == procedure_version

        # Optional fields
        if docstring:
            assert run.docstring == docstring

        return run

    return func


@pytest.fixture()
def extract_id_and_check_run_exists(extract_run_id_from_logs, check_run_exists):
    """Function to check if a run exists (extracted from the logs), returns the run for further checks"""

    def func(
        *,
        # Basic run fields
        outcome: Union[str, None] = None,
        # Unit fields
        serial_number: Union[str, None] = None,
        part_number: Union[str, None] = None,
        part_name: Union[str, None] = None,
        revision: Union[str, None] = None,
        batch_number: Union[str, None] = None,
        # Procedure fields
        procedure_id: Union[str, None] = None,
        procedure_name: Union[str, None] = None,
        procedure_version: Union[str, None] = None,
        # Optional fields
        docstring: Union[str, None] = None,
    ):
        id = extract_run_id_from_logs()
        return check_run_exists(
            id,
            outcome=outcome,
            serial_number=serial_number,
            part_number=part_number,
            part_name=part_name,
            revision=revision,
            batch_number=batch_number,
            procedure_id=procedure_id,
            procedure_name=procedure_name,
            procedure_version=procedure_version,
            docstring=docstring,
        )

    return func
