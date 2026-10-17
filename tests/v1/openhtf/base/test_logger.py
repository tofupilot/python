"""Adapted from the former tofupilot/examples repository (testing_examples/openhtf/logger)"""

import random

import openhtf as htf
from openhtf.output.callbacks import json_factory
from tofupilot.openhtf import upload


@htf.measures(htf.Measurement("button_status").equals(True))
def phase_info_logger(test):
    test.measurements.button_status = True
    test.logger.info("Logging an info in the logger")


def phase_error_logger(test):
    test.logger.error("Logging error in the logger")
    return htf.PhaseResult.CONTINUE


def phase_warning_logger(test):
    test.logger.warning("Logging a warning in the logger")
    return htf.PhaseResult.CONTINUE


def phase_critical_logger(test):
    test.logger.critical("Logging a critical in the logger")
    return htf.PhaseResult.CONTINUE


def phase_debug_logger(test):
    test.logger.debug("Logging a debug in the logger")
    return htf.PhaseResult.CONTINUE


def test_logger(tofupilot_server_url, api_key, procedure_identifier, procedure_id, tmp_path, extract_id_and_check_run_exists):
    # Define the test plan with all steps
    test = htf.Test(
        phase_info_logger,
        phase_error_logger,
        phase_warning_logger,
        phase_critical_logger,
        phase_debug_logger,
        procedure_id=procedure_identifier,
        part_number="00220D",
    )

    # Generate random Serial Number
    random_digits = "".join([str(random.randint(0, 9)) for _ in range(5)])
    serial_number = f"00220D4K{random_digits}"

    test.add_output_callbacks(
        json_factory.OutputToJSON(
            str(tmp_path) + "test_result.json", indent=2))

    # Execute the test
    test.add_output_callbacks(upload(url=tofupilot_server_url, api_key=api_key))
    test.execute(lambda: serial_number)

    run = extract_id_and_check_run_exists(serial_number=serial_number, procedure_id=procedure_id, part_number="00220D")

    assert run.logs
    assert len([l for l in run.logs if l.level == "INFO"     and l.message == "Logging an info in the logger"    ]) == 1
    assert len([l for l in run.logs if l.level == "ERROR"    and l.message == "Logging error in the logger"      ]) == 1
    assert len([l for l in run.logs if l.level == "WARNING"  and l.message == "Logging a warning in the logger"  ]) == 1
    assert len([l for l in run.logs if l.level == "CRITICAL" and l.message == "Logging a critical in the logger" ]) == 1
    assert len([l for l in run.logs if l.level == "DEBUG"    and l.message == "Logging a debug in the logger"    ]) == 1