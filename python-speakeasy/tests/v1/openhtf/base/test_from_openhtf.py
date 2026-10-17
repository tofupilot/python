"""Test run creation from OpenHTF reports."""

# Adapted from the former tofupilot/examples repository (qa/client/create_run_from_openhtf_report/basic/main.py)
import pytest
import logging
from typing import Generator

from openhtf import PhaseResult, Test as Openhtf_Test, measures, Measurement
from openhtf.core.test_descriptor import TestApi
from _pytest.logging import LogCaptureFixture
from tofupilot.openhtf import upload


# Define a test phase to simulate the power-on procedure
def power_on_test(test: TestApi) -> PhaseResult:
    print("Power on.")
    return PhaseResult.CONTINUE


# Define a phase that attaches a file
def phase_file_attachment(test: TestApi) -> PhaseResult:
    test.attach_from_file("tests/v1/attachments/sample_file.txt")
    test.attach_from_file("tests/v1/attachments/oscilloscope.jpeg")
    return PhaseResult.CONTINUE


# Multi-dimensional measurement test based on OpenHTF examples
@measures(
    Measurement('power_time_series').with_dimensions('time_ms', 'voltage', 'current'),
    Measurement('temperature_profile').with_dimensions('sensor_id', 'reading_number'),
    Measurement('frequency_response').with_dimensions('frequency_hz'),
    Measurement('average_power').with_units('W'),
    Measurement('max_temperature').with_units('°C'),
    Measurement('test_duration').with_units('ms')
)
def multidimensional_measurement_phase(test: TestApi) -> PhaseResult:
    """Phase that demonstrates multi-dimensional measurements from OpenHTF."""
    
    # Multi-dimensional power measurements over time
    # Simulate power measurements at different time points with varying voltage and current
    for i in range(5):
        time_ms = i * 100  # 0, 100, 200, 300, 400 ms
        voltage = 12.0 + (i * 0.5)  # 12.0, 12.5, 13.0, 13.5, 14.0 V
        current = 2.0 + (i * 0.1)   # 2.0, 2.1, 2.2, 2.3, 2.4 A
        
        # Set multi-dimensional measurement with 3 dimensions
        dimensions = (time_ms, voltage, current)
        test.measurements['power_time_series'][dimensions] = voltage * current
    
    # Multi-dimensional temperature measurements from multiple sensors
    sensor_names = ['CPU_TEMP', 'GPU_TEMP', 'AMBIENT_TEMP']
    for sensor_id in sensor_names:
        for reading_num in range(3):
            # Simulate different temperature patterns for each sensor
            if sensor_id == 'CPU_TEMP':
                temp = 45.0 + reading_num * 5.0  # 45, 50, 55°C
            elif sensor_id == 'GPU_TEMP':
                temp = 40.0 + reading_num * 3.0  # 40, 43, 46°C
            else:  # AMBIENT_TEMP
                temp = 22.0 + reading_num * 1.0  # 22, 23, 24°C
            
            test.measurements['temperature_profile'][(sensor_id, reading_num)] = temp
    
    # Single-dimensional array: frequency response measurements
    frequencies = [100.0, 1000.0, 10000.0, 100000.0]  # Hz
    responses = [0.95, 0.98, 0.85, 0.60]  # Response ratios
    
    for freq, response in zip(frequencies, responses):
        test.measurements['frequency_response'][freq] = response
    
    # Calculate summary measurements from multi-dimensional data
    power_measurements = test.measurements['power_time_series']
    if power_measurements.is_value_set:
        # Extract power values and calculate average
        power_values = [measurement[-1] for measurement in power_measurements.value]
        test.measurements['average_power'] = sum(power_values) / len(power_values)
    
    # Find maximum temperature across all sensors
    temp_measurements = test.measurements['temperature_profile']
    if temp_measurements.is_value_set:
        temp_values = [measurement[-1] for measurement in temp_measurements.value]
        test.measurements['max_temperature'] = max(temp_values)
    
    # Record test duration
    test.measurements['test_duration'] = 500  # ms
    
    return PhaseResult.CONTINUE

@pytest.fixture(autouse=True)
def no_error_logs(caplog: LogCaptureFixture) -> Generator[None, None, None]:
    yield
    errors = [record for record in caplog.get_records('call') if record.levelno >= logging.ERROR]
    assert not errors, f"{errors[0].message}"

class TestCreateRunFromOpenHTF:

    def test_basic_openhtf_run_creation(self, tofupilot_server_url: str, api_key: str, procedure_identifier: str) -> None:
        """Test basic OpenHTF run creation."""

        test = Openhtf_Test(power_on_test, serial_number="PCB01", procedure_id=procedure_identifier, part_number="test_basic")

        # Execute the test with a specific device identifier
        test.add_output_callbacks(upload(url=tofupilot_server_url, api_key=api_key))
        test.execute(lambda: "0001")

    def test_openhtf_run_creation_with_attachments(self, tofupilot_server_url: str, api_key: str, procedure_identifier: str) -> None:
        """Test OpenHTF run creation with file attachments."""
        # Adapted from the former tofupilot/examples repository (qa/client/create_run_from_openhtf_report/with_attachments/main.py)
        
        test = Openhtf_Test(
            power_on_test,
            phase_file_attachment,
            procedure_id=procedure_identifier,
            part_number="test_with_attachments",
            serial_number="PCB01")

        # Execute the test with a specific device identifier
        test.add_output_callbacks(upload(url=tofupilot_server_url, api_key=api_key))
        test.execute(lambda: "0001")

    def test_openhtf_multidimensional_measurements(self, tofupilot_server_url: str, api_key: str, procedure_identifier: str) -> None:
        """Test OpenHTF with multi-dimensional measurements upload to TofuPilot."""
        
        test = Openhtf_Test(
            power_on_test,
            multidimensional_measurement_phase,
            procedure_id=procedure_identifier,
            part_number="test_multidim",
            serial_number="MULTIDIM_PCB_001"
        )

        # Execute the test with a specific device identifier
        test.add_output_callbacks(upload(url=tofupilot_server_url, api_key=api_key))
        test.execute(lambda: "MULTIDIM_001")

    def test_upload_callback_without_context_manager(self, tofupilot_server_url: str, api_key: str, procedure_identifier: str) -> None:
        """Test OpenHTF run creation via the upload callback."""
        from tofupilot.openhtf import upload

        test = Openhtf_Test(power_on_test, procedure_id=procedure_identifier, part_number="test_upload_cb")
        test.add_output_callbacks(upload(api_key=api_key, url=tofupilot_server_url))
        test.execute(lambda: "0001")

    def test_deprecated_context_manager_still_uploads(self, tofupilot_server_url: str, api_key: str, procedure_identifier: str, extract_id_and_check_run_exists) -> None:
        """The deprecated TofuPilot context manager warns but still uploads the run."""
        from tofupilot.openhtf import TofuPilot

        test = Openhtf_Test(power_on_test, procedure_id=procedure_identifier, part_number="test_ctx_manager")
        with pytest.warns(FutureWarning, match="deprecated"):
            ctx = TofuPilot(test, stream=False, url=tofupilot_server_url, api_key=api_key)
        with ctx:
            test.execute(lambda: "0001")

        # The callback must be deregistered on exit, or the classic station
        # loop (`for sn in queue: with TofuPilot(test): ...`) stacks one
        # callback per iteration and uploads run N a total of N times.
        assert ctx._upload not in test._test_options.output_callbacks

        # OpenHTF swallows output-callback exceptions, so a green execute()
        # proves nothing about the upload; verify the run actually landed.
        extract_id_and_check_run_exists(serial_number="0001", part_number="test_ctx_manager")

    def test_create_run_from_openhtf_json_report(self, tofupilot_server_url: str, api_key: str, procedure_identifier: str, check_run_exists, tmp_path) -> None:
        """Test run creation from a pre-existing OpenHTF JSON report file."""
        from tofupilot.v1.client import TofuPilotClient
        from openhtf.output.callbacks import json_factory

        # Run an OpenHTF test and save JSON report to disk
        test = Openhtf_Test(power_on_test, procedure_id=procedure_identifier, part_number="test_json_import")
        report_path = str(tmp_path / "report.json")
        test.add_output_callbacks(json_factory.OutputToJSON(report_path, indent=4))
        test.execute(lambda: "0001")

        # Import the saved JSON report via the client method
        client = TofuPilotClient(api_key=api_key, url=tofupilot_server_url)
        run_id = client.create_run_from_openhtf_report(report_path)

        # Verify the run was created
        assert run_id
        check_run_exists(run_id, serial_number="0001", part_number="test_json_import")