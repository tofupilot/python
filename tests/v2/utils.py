import pytest
import os
from datetime import datetime, timedelta, timezone
from typing import Union, List, Tuple
import contextlib
import random

from trycast import checkcast

# Import the new SDK
from tofupilot.v2 import TofuPilot
from tofupilot.v2 import models
from tofupilot.v2.errors import APIError, TofuPilotError


@pytest.fixture(scope="class")
def client(api_key: str, tofupilot_server_url: str) -> TofuPilot:
    """Create TofuPilot client with the new SDK."""
    client = TofuPilot(
        api_key=api_key,
        server_url=f"{tofupilot_server_url}/api",
    )
    
    return client

@pytest.fixture(scope="class")
def user_client(user_api_key, tofupilot_server_url) -> TofuPilot:
    return TofuPilot(
        api_key=user_api_key,
        server_url=f"{tofupilot_server_url}/api",
    )

@pytest.fixture(scope="class")
def station_client(station_api_key, tofupilot_server_url) -> TofuPilot:
    return TofuPilot(
        api_key=station_api_key,
        server_url=f"{tofupilot_server_url}/api",
    )

@pytest.fixture
def operator_email_address() -> str:
    # Get API key from environment
    api_key = os.environ.get("TOFUPILOT_EMAIL_ADDRESS_USER")
    
    if not api_key:
        pytest.fail(
            "TOFUPILOT_EMAIL_ADDRESS_USER environment variable not set. "
            "Set it to run tests against local server:\n"
            "export TOFUPILOT_EMAIL_ADDRESS_USER='email-of-your-user'"
        )
    return api_key

def assert_get_run_success(result: models.RunGetResponse) -> None:
    """Assert that run get response is valid."""
    assert checkcast(models.RunGetResponse, result)
    assert len(result.id) > 0

def assert_delete_run_success(result: models.RunDeleteResponse) -> None:
    """Assert that run delete response is valid."""
    assert checkcast(models.RunDeleteResponse, result)
    assert len(result.id) > 0

def assert_update_run_success(result: models.RunUpdateResponse) -> None:
    """Assert that run update response is valid."""
    assert checkcast(models.RunUpdateResponse, result)
    assert len(result.id) > 0

def assert_get_runs_success(result: models.RunListResponse) -> None:
    """Assert that runs list response is valid."""
    assert checkcast(models.RunListResponse, result)
    for run in result.data:
        assert len(run.id) > 0

def assert_create_run_success(result: models.RunCreateResponse) -> None:
    # Check that the result has the expected structure
    assert checkcast(models.RunCreateResponse, result)
    assert len(result.id) > 0

def assert_get_units_success(result: models.UnitListResponse) -> None:
    assert checkcast(models.UnitListResponse, result)

def assert_delete_units_success(result: models.UnitDeleteResponse) -> None:
    assert checkcast(models.UnitDeleteResponse, result)

def assert_timestamps_close(
    actual: Union[datetime, str], 
    expected: Union[datetime, str], 
    tolerance_seconds: float = 0.001,
    message: str = "Timestamps should be close"
) -> None:
    """
    Assert that two timestamps are close within a tolerance.
    
    This handles the common issue where server-side timestamp processing
    can introduce small differences due to:
    - Database datetime precision limitations
    - ISO string conversion precision loss
    - Timezone handling differences
    
    Args:
        actual: The actual timestamp (from server response)
        expected: The expected timestamp (from test input)
        tolerance_seconds: Maximum allowed difference in seconds (default: 0.001 = 1ms)
        message: Custom assertion message
    """
    # Convert to datetime objects if needed
    if isinstance(actual, str):
        actual = datetime.fromisoformat(actual.replace('Z', '+00:00'))
    if isinstance(expected, str):
        expected = datetime.fromisoformat(expected.replace('Z', '+00:00'))
    
    # Remove timezone info for comparison if one has it and the other doesn't
    if actual.tzinfo is not None and expected.tzinfo is None:
        actual = actual.replace(tzinfo=None)
    elif actual.tzinfo is None and expected.tzinfo is not None:
        expected = expected.replace(tzinfo=None)
    
    # Calculate the time difference
    time_diff = abs((actual - expected).total_seconds())
    
    assert time_diff <= tolerance_seconds, (
        f"{message}. Difference: {time_diff:.6f} seconds "
        f"(tolerance: {tolerance_seconds}s). "
        f"Actual: {actual}, Expected: {expected}"
    )

@contextlib.contextmanager
def assert_station_access_forbidden(operation_description: str = "operation"):
    """
    Context manager to assert that station access is properly forbidden with HTTP 403.
    
    This should be used for operations where stations have 'unauthorized' access level
    in x-access configuration, which should be caught by the access control middleware
    and return HTTP 403 FORBIDDEN.
    
    Args:
        operation_description: Description of the operation being tested for error messages
        
    Usage:
        with assert_station_access_forbidden("create procedure"):
            client.procedures.create(name="test")
    """
    # TofuPilotError, not APIError: once an endpoint declares its 403 in the spec,
    # the SDK raises the typed ErrorFORBIDDEN, which is a sibling of APIError.
    with pytest.raises(TofuPilotError) as exc_info:
        yield

    api_error = exc_info.value

    # Verify HTTP 403 FORBIDDEN status code
    assert hasattr(api_error, 'status_code'), (
        f"TofuPilotError should have status_code attribute for {operation_description}. "
        f"Error: {api_error}"
    )
    assert api_error.status_code == 403, (
        f"Expected HTTP 403 FORBIDDEN for station {operation_description}, "
        f"got: {api_error.status_code}. Full error: {api_error}"
    )
    
    # Verify error message indicates access control issue
    error_message = str(api_error).lower()
    access_control_keywords = [
        "stations cannot",          # Our x-access descriptions  
        "station access is not authorized",  # Generic middleware message
        "forbidden",                # HTTP status meaning
        "access denied for station", # Middleware message for missing rule
        "unauthorized",             # General access control terms
        "permission",               # Permission-related errors
    ]
    
    assert any(keyword in error_message for keyword in access_control_keywords), (
        f"Expected access control error message for station {operation_description} "
        f"containing one of {access_control_keywords}, but got: {api_error}"
    )

@contextlib.contextmanager  
def assert_station_access_limited(operation_description: str = "operation"):
    """
    Context manager for operations where stations have 'limited' access.
    
    This is for operations where the access control is handled by the business logic
    rather than the middleware, so the HTTP status and error types may vary.
    
    Args:
        operation_description: Description of the operation being tested
        
    Usage:
        with assert_station_access_limited("create run for unlinked procedure"):
            client.runs.create(...)
    """
    with pytest.raises(APIError) as exc_info:
        yield
    
    # For limited access, we expect some kind of error but the specific type
    # depends on the business logic implementation
    error = exc_info.value
    assert error is not None, f"Expected some error for limited station access to {operation_description}"
    
    # Log the error for debugging but don't enforce specific format
    # since limited access errors are handled by business logic
    print(f"Station limited access error for {operation_description}: {error}")


def get_random_test_dates(duration_minutes: int = 5) -> Tuple[datetime, datetime]:
    """
    Generate random started_at and ended_at dates within the last 7 days.
    
    This helps ensure test data appears recent and realistic while avoiding
    hardcoded dates that become stale over time.
    
    Args:
        duration_minutes: How long the run should take (default: 5 minutes)
        
    Returns:
        Tuple of (started_at, ended_at) datetime objects with UTC timezone
    """
    # Get current time
    now = datetime.now(timezone.utc)
    
    # Calculate random start time within last 7 days
    # Random offset between 0 and 7 days (in minutes)
    max_offset_minutes = 7 * 24 * 60
    random_offset_minutes = random.randint(duration_minutes, max_offset_minutes)
    
    # Calculate started_at by subtracting the random offset
    started_at = now - timedelta(minutes=random_offset_minutes)
    
    # Calculate ended_at by adding the duration
    ended_at = started_at + timedelta(minutes=duration_minutes)
    
    return started_at, ended_at