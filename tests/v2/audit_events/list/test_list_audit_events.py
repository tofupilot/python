"""GET /v2/audit-events: the organization's audit trail (public V2 endpoint)."""

from typing import Optional

from tofupilot.v2 import TofuPilot
from tofupilot.v2.errors import TofuPilotError
from ...utils import assert_create_run_success, assert_station_access_forbidden, get_random_test_dates
from ....e2e_tag import uid


def _list_or_plan_refusal(client: TofuPilot, **filters) -> Optional[object]:
    """The page, or None when the test organization's plan does not include the audit log API.

    The endpoint is Enterprise only: on another plan the refusal itself is what
    is checked, with its documented message.
    """
    try:
        return client.audit_log.list(**filters)
    except TofuPilotError as error:
        assert error.status_code == 403, f"Expected the plan refusal, got {error.status_code}: {error}"
        assert "Enterprise plan" in str(error), f"Expected the Enterprise plan message, got: {error}"
        return None


class TestListAuditEvents:
    def test_station_keys_are_refused(self, client: TofuPilot, auth_type: str) -> None:
        if auth_type == "station":
            with assert_station_access_forbidden("list audit events"):
                client.audit_log.list(limit=1)
            return
        _list_or_plan_refusal(client, limit=1)

    def test_a_write_is_on_the_trail_and_pages_with_a_cursor(
        self, client: TofuPilot, auth_type: str, procedure_id: str
    ) -> None:
        if auth_type == "station":
            return  # Refusal covered above; this test reads the trail as a user.
        serial = f"SN-AUDIT-{uid()}"
        started_at, ended_at = get_random_test_dates()
        for _ in range(2):
            run = client.runs.create(
                serial_number=serial,
                procedure_id=procedure_id,
                part_number=f"PART-AUDIT-{uid()}",
                started_at=started_at,
                ended_at=ended_at,
                outcome="PASS",
            )
            assert_create_run_success(run)

        page = _list_or_plan_refusal(client, actions=["run.create"], search_query=serial, limit=1)
        if page is None:
            return
        assert len(page.data) == 1
        event = page.data[0]
        assert event.action == "run.create"
        assert event.outcome == "success"
        assert event.via == "api"
        assert event.actor.type == "user"
        assert page.meta.has_more is True
        assert page.meta.next_cursor

        second = client.audit_log.list(
            actions=["run.create"], search_query=serial, limit=1, cursor=page.meta.next_cursor
        )
        assert len(second.data) == 1
        assert second.data[0].id != event.id

    def test_an_altered_cursor_is_refused(self, client: TofuPilot, auth_type: str) -> None:
        if auth_type == "station":
            return
        if _list_or_plan_refusal(client, limit=1) is None:
            return
        try:
            client.audit_log.list(limit=1, cursor="not-a-cursor")
        except TofuPilotError as error:
            assert error.status_code == 400
        else:
            raise AssertionError("an altered cursor must be refused with 400")
