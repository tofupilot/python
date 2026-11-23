"""Importing a bench log again after rows were appended to it (TP-1457).

The runs an earlier import created are skipped and reported, whatever the
mapping; only the appended rows become runs.
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


def _mapping(tag: str, time_zone: str = "UTC") -> dict:
    return {
        "runsPer": "row",
        "timeZone": time_zone,
        "runKeyColumns": ["serial_number", "test_date"],
        "fields": {
            "serial_number": {"column": "serial_number"},
            "part_number": {"value": f"GROW-{tag}"},
            "outcome": {
                "column": "verdict",
                "valueMap": {"passed": "PASS", "failed": "FAIL"},
            },
            "started_at": {"column": "test_date"},
            "ended_at": {"column": "test_date"},
        },
        "measurements": {"layout": "none"},
    }


class TestTabularImportGrownFile:
    def test_only_appended_rows_are_created(
        self, client: TofuPilot, procedure_id: str
    ) -> None:
        tag = uid()
        header = "serial_number,test_date,verdict\n"
        first_rows = (
            f"SN-1-{tag},2025-06-01,passed\n" f"SN-2-{tag},2025-06-01,passed\n"
        )

        first = client.imports.tabular(
            upload_id=_upload(client, f"log-{tag}.csv", header + first_rows),
            procedure_id=procedure_id,
            mapping=_mapping(tag),
        )
        assert first.ids is not None and len(first.ids) == 2
        assert first.skipped_row_count == 0

        # The tester appended a row and someone edited row 1 in place; the
        # file is imported again with another time zone in the mapping.
        grown = (
            header
            + f"SN-1-{tag},2025-06-01,failed\n"
            + f"SN-2-{tag},2025-06-01,passed\n"
            + f"SN-3-{tag},2025-06-02,passed\n"
        )
        again = client.imports.tabular(
            upload_id=_upload(client, f"log-{tag}.csv", grown),
            procedure_id=procedure_id,
            mapping=_mapping(tag, "Europe/Berlin"),
        )
        assert again.already_imported is False
        assert again.ids is not None and len(again.ids) == 1
        assert again.skipped_row_count == 2
        assert again.skipped_rows is not None
        reasons = {r.row: (r.reason, r.run_id) for r in again.skipped_rows}
        assert reasons[1][0] == "changed"
        assert reasons[2][0] == "already_imported"
        assert {r.run_id for r in again.skipped_rows} == set(first.ids)

        # The edited row's run was kept as it was.
        kept = client.runs.get(id=reasons[1][1])
        assert kept.outcome == "PASS"

        # Nothing new: every run of the request existed.
        third = client.imports.tabular(
            upload_id=_upload(client, f"log-{tag}.csv", grown),
            procedure_id=procedure_id,
            mapping=_mapping(tag),
        )
        assert third.already_imported is True
        assert third.skipped_row_count == 3
