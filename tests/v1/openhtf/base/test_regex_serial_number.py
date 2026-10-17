"""Test that omitting part_number without server-side regex config produces a clear error.

Adapted from the former tofupilot/examples repository (testing_examples/openhtf/regex_serial_number)
"""

import logging

import openhtf as htf
from tofupilot.openhtf import upload


@htf.measures(htf.Measurement("button_status").equals(True))
def check_button(test):
    test.measurements.button_status = False


def test_no_part_number_without_regex_config(tofupilot_server_url, api_key, procedure_identifier, caplog):
    """When no part_number is provided and no regex parsing is configured, the server should reject the run."""
    test = htf.Test(
        check_button,
        procedure_id=procedure_identifier,
    )

    test.add_output_callbacks(upload(url=tofupilot_server_url, api_key=api_key))
    test.execute(lambda: "PCB101T5A123")

    errors = [
        record for record in caplog.get_records("call")
        if record.levelno >= logging.ERROR
    ]
    assert any("part number" in record.getMessage().lower() for record in errors), (
        f"Expected an error about missing part number, got: {[r.getMessage() for r in errors]}"
    )
