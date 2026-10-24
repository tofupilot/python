"""Test the `samples` filter on units and runs lists, `production` included."""

from tofupilot.v2 import TofuPilot
from ..utils import assert_create_unit_success, assert_get_units_success
from ...utils import assert_create_run_success, assert_get_runs_success, get_random_test_dates


class TestListSamples:
    """`production` (no sample classification) combines with the reference classes."""

    def _create_units(self, client: TofuPilot, timestamp: str) -> tuple[str, str, dict[str, str]]:
        part_number = f"SAMPLES-PART-{timestamp}"
        revision_number = f"SAMPLES-REV-{timestamp}"
        client.parts.create(number=part_number, name=f"Test Part SAMPLES {timestamp}")
        client.parts.revisions.create(part_number=part_number, number=revision_number)

        serials: dict[str, str] = {}
        for cls in ("production", "golden", "failing"):
            serial = f"SAMPLES-{cls[:4].upper()}-{timestamp}"
            result = client.units.create(
                serial_number=serial,
                part_number=part_number,
                revision_number=revision_number,
                sample=None if cls == "production" else cls,
            )
            assert_create_unit_success(result)
            serials[cls] = serial
        return part_number, revision_number, serials

    def test_units_list_samples(self, client: TofuPilot, timestamp: str) -> None:
        part_number, _, serials = self._create_units(client, timestamp)

        def listed(samples: list) -> set[str]:
            result = client.units.list(part_numbers=[part_number], samples=samples)
            assert_get_units_success(result)
            return {u.serial_number for u in result.data}

        assert listed(["production"]) == {serials["production"]}
        assert listed(["production", "golden"]) == {serials["production"], serials["golden"]}
        assert listed(["golden", "failing"]) == {serials["golden"], serials["failing"]}

    def test_runs_list_samples(self, client: TofuPilot, procedure_id: str, timestamp: str) -> None:
        part_number, revision_number, serials = self._create_units(client, timestamp)
        run_ids: dict[str, str] = {}
        for cls, serial in serials.items():
            started_at, ended_at = get_random_test_dates()
            result = client.runs.create(
                serial_number=serial,
                procedure_id=procedure_id,
                part_number=part_number,
                revision_number=revision_number,
                started_at=started_at,
                ended_at=ended_at,
                outcome="PASS",
            )
            assert_create_run_success(result)
            run_ids[cls] = result.id

        def listed(samples: list) -> set[str]:
            result = client.runs.list(part_numbers=[part_number], samples=samples)
            assert_get_runs_success(result)
            return {r.id for r in result.data}

        assert listed(["production"]) == {run_ids["production"]}
        assert listed(["production", "golden"]) == {run_ids["production"], run_ids["golden"]}
