"""`*` wildcards on the identifier filters of GET /v2/runs, through the REST layer."""

from datetime import datetime, timezone

import pytest
from tofupilot.v2 import TofuPilot
from tofupilot.v2.errors import ErrorBADREQUEST
from ....e2e_tag import uid


@pytest.fixture(scope="class")
def fixture(client: TofuPilot, procedure_id: str) -> tuple[str, list[str]]:
    # Class-scoped, so the prefix comes from uid() rather than the
    # function-scoped `timestamp` fixture.
    prefix = f"WC{uid()}"
    # `A_1` and `AX1` differ only where LIKE would read `_` as "any character".
    serials = [f"{prefix}-A_1", f"{prefix}-AX1", f"{prefix}-B-01"]
    now = datetime.now(timezone.utc)
    for sn in serials:
        client.runs.create(
            outcome="PASS",
            procedure_id=procedure_id,
            serial_number=sn,
            part_number=f"{prefix}-PART",
            started_at=now,
            ended_at=now,
        )
    return prefix, serials


class TestRunsWildcardFilters:
    """Values without `*` stay exact and case-insensitive; `*` matches any run of characters."""

    def _serials(self, client: TofuPilot, procedure_id: str, patterns: list[str]) -> list[str]:
        res = client.runs.list(procedure_ids=[procedure_id], serial_numbers=patterns, limit=100)
        return sorted(run.unit.serial_number for run in res.data)

    def test_exact_value_is_unchanged(self, client: TofuPilot, procedure_id: str, fixture) -> None:
        _, serials = fixture
        assert self._serials(client, procedure_id, [serials[2].lower()]) == [serials[2]]
        assert self._serials(client, procedure_id, [serials[2][:-1]]) == []

    def test_trailing_star_matches_the_prefix(self, client: TofuPilot, procedure_id: str, fixture) -> None:
        prefix, serials = fixture
        assert self._serials(client, procedure_id, [f"{prefix}-*"]) == sorted(serials)
        assert self._serials(client, procedure_id, [f"{prefix.lower()}-b-*"]) == [serials[2]]

    def test_underscore_is_literal(self, client: TofuPilot, procedure_id: str, fixture) -> None:
        prefix, serials = fixture
        assert self._serials(client, procedure_id, [f"{prefix}-A_*"]) == [serials[0]]

    def test_leading_star_is_rejected(self, client: TofuPilot, procedure_id: str) -> None:
        with pytest.raises(ErrorBADREQUEST) as exc_info:
            client.runs.list(procedure_ids=[procedure_id], serial_numbers=["*-B-01"])
        assert "leading `*`" in str(exc_info.value)
