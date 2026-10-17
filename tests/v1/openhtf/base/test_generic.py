"""Adapted from the former tofupilot/examples repository (testing_examples/openhtf/generic)"""

import random

import openhtf as htf
from openhtf.util import units
from tofupilot.openhtf import upload


@htf.measures(htf.Measurement("firmware_version").equals("1.4.3"))
def pcba_firmware_version(test):
    test.measurements.firmware_version = "1.4.3"


@htf.measures(htf.Measurement("button_status").equals(True))
def check_button(test):
    test.measurements.button_status = True


@htf.measures(htf.Measurement("input_voltage").in_range(4.5,
              5).with_units(units.VOLT))
def check_voltage_input(test):
    test.measurements.input_voltage = 4.7


@htf.measures(
    htf.Measurement("output_voltage").in_range(3.2, 3.4).with_units(units.VOLT)
)
def check_voltage_output(test):
    test.measurements.output_voltage = 3.3


@htf.measures(
    htf.Measurement("current_protection_triggered")
    .in_range(maximum=1.5)
    .with_units(units.AMPERE)
)
def check_overcurrent_protection(test):
    test.measurements.current_protection_triggered = 1.2


def check_battery_switch():
    return htf.PhaseResult.CONTINUE


@htf.measures(
    htf.Measurement("efficiency").in_range(85, 98).with_units(units.Unit("%"))
)
def check_converter_efficiency(test):
    input_power = 500
    output_power = 450
    test.measurements.efficiency = round(
        ((output_power / input_power) * 100), 1)


def visual_control_pcb_coating(test):
    test.attach_from_file("tests/v1/attachments/oscilloscope.jpeg")
    return htf.PhaseResult.CONTINUE


def test_generic(tofupilot_server_url, api_key, procedure_identifier, procedure_id, extract_id_and_check_run_exists):
    # Define the test plan with all steps
    test = htf.Test(
        pcba_firmware_version,
        check_button,
        check_voltage_input,
        check_voltage_output,
        check_overcurrent_protection,
        check_battery_switch,
        check_converter_efficiency,
        visual_control_pcb_coating,
        procedure_id=procedure_identifier,
        part_number="00220",
        revision="A",
        batch_number="1024-0001",
        # sub_units=[{"serial_number": "00102"}],
    )

    # Generate random Serial Number
    random_digits = "".join([str(random.randint(0, 9)) for _ in range(5)])
    serial_number = f"00220B4K{random_digits}"

    # Execute the test
    test.add_output_callbacks(upload(url=tofupilot_server_url, api_key=api_key))
    test.execute(lambda: serial_number)

    
    extract_id_and_check_run_exists(serial_number=serial_number, procedure_id=procedure_id, outcome="PASS", part_number="00220", revision="A", batch_number="1024-0001")
