"""Tabular import with one run per row (TP-1418).

A bench log appends one row per test: several units per file, retests of a
unit on later rows, timestamps without an offset. Module runs are imported
first, then a tray log whose rows name those modules as sub-units.
"""

import requests as http_requests
from tofupilot.v2 import TofuPilot

from ...e2e_tag import uid


def _upload(client: TofuPilot, name: str, text: str) -> str:
    init = client.attachments.initialize(name=name)
    resp = http_requests.put(
        init.upload_url, data=text.encode(), headers={"Content-Type": "text/csv"}
    )
    assert resp.status_code == 200
    client.attachments.finalize(id=init.id)
    return init.id


class TestTabularImportPerRow:
    def test_module_then_tray_log(self, client: TofuPilot, procedure_id: str) -> None:
        tag = uid()
        mod_a, mod_b, tray_ok, tray_bad = (
            f"MOD-A-{tag}",
            f"MOD-B-{tag}",
            f"TRAY-OK-{tag}",
            f"TRAY-BAD-{tag}",
        )

        # Module log: date-only timestamps, a retest of mod_a, one bad verdict.
        module_csv = (
            "serial_number,test_date,filename_verdict,dc_link_voltage\n"
            f"{mod_a},2025-03-10,failed,398.1\n"
            f"{mod_a},2025-03-11,passed,398.7\n"
            f"{mod_b},2025-03-11,passed,399.0\n"
            f"MOD-C-{tag},2025-03-11,exploded,1\n"
        )
        modules = client.imports.tabular(
            upload_id=_upload(client, f"modules-{tag}.csv", module_csv),
            procedure_id=procedure_id,
            mapping={
                "runsPer": "row",
                "timeZone": "Europe/Berlin",
                "fields": {
                    "serial_number": {"column": "serial_number"},
                    "part_number": {"value": f"INV-{tag}"},
                    "outcome": {
                        "column": "filename_verdict",
                        "valueMap": {"passed": "PASS", "failed": "FAIL"},
                    },
                    "started_at": {"column": "test_date"},
                    "ended_at": {"column": "test_date"},
                },
                "measurements": {
                    "layout": "wide",
                    "columns": [{"column": "dc_link_voltage", "units": "V"}],
                },
            },
        )
        assert modules.ids is not None and len(modules.ids) == 3
        assert modules.total_runs == 4
        assert modules.next_run_offset is None
        assert modules.failed_row_count == 1
        assert modules.row_errors is not None
        assert modules.row_errors[0].row == 4
        assert "exploded" in modules.row_errors[0].error

        first = client.runs.get(id=modules.ids[0])
        assert first.outcome == "FAIL"
        # Midnight in Berlin is 23:00 UTC the day before.
        assert first.started_at.isoformat().startswith("2025-03-09T23:00:00")

        # Tray log: a naive timestamp with microseconds, modules as sub-units,
        # one row naming a module that does not exist.
        tray_csv = (
            "project_name,tray_barcode,timestep,module_barcode_1,module_barcode_2\n"
            f"TRAY-{tag},{tray_ok},2026-07-08T15:49:48.349659,{mod_a},{mod_b}\n"
            f"TRAY-{tag},{tray_bad},2026-07-08T16:00:00,MISSING-{tag},\n"
        )
        trays = client.imports.tabular(
            upload_id=_upload(client, f"trays-{tag}.csv", tray_csv),
            procedure_id=procedure_id,
            mapping={
                "runsPer": "row",
                "fields": {
                    "serial_number": {"column": "tray_barcode"},
                    "part_number": {"column": "project_name"},
                    "outcome": {"value": "PASS"},
                    "started_at": {"column": "timestep"},
                    "ended_at": {"column": "timestep"},
                },
                "measurements": {"layout": "none"},
                "subUnitColumns": ["module_barcode_1", "module_barcode_2"],
            },
        )
        assert trays.ids is not None and len(trays.ids) == 1
        assert trays.row_errors is not None
        assert trays.row_errors[0].row == 2
        assert trays.row_errors[0].serial_number == tray_bad
        assert f"Sub-unit not found: MISSING-{tag}" in trays.row_errors[0].error

        tray_unit = client.units.get(serial_number=tray_ok)
        children = sorted(c.serial_number for c in (tray_unit.children or []))
        assert children == sorted([mod_a, mod_b])
