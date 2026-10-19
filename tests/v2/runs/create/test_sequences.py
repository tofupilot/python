"""Sequence calls on a run: the tree sent on create, read back on get, list
and phases.list, and the validation of a nested call without its parent."""

import pytest
from tofupilot.v2 import TofuPilot
from tofupilot.v2.errors import ErrorBADREQUEST
from ...utils import get_random_test_dates, assert_create_run_success
from ....e2e_tag import uid


def _create_run_with_sequences(client: TofuPilot, procedure_id: str, unique: str):
    """A top-level call, a call nested in it, a skipped call, and one
    procedure-level phase. Returns (run id, phases payload)."""
    started_at, ended_at = get_random_test_dates()
    sequences = [
        {
            "path": "calib",
            "name": "Calibration",
            "source": "sequences/calib.yaml",
            "outcome": "FAIL",
            "started_at": started_at,
            "ended_at": ended_at,
        },
        {
            "path": "calib/dmm",
            "name": "DMM",
            "source": "dmm.yaml",
            "outcome": "FAIL",
            "started_at": started_at,
            "ended_at": ended_at,
        },
        # Never ran: no times.
        {"path": "final", "name": "Final check", "outcome": "SKIP"},
    ]
    phases = [
        {"name": "warmup", "outcome": "PASS", "started_at": started_at, "ended_at": ended_at},
        {
            "name": "zero",
            "outcome": "PASS",
            "started_at": started_at,
            "ended_at": ended_at,
            "sequence_path": "calib",
        },
        {
            "name": "check",
            "outcome": "PASS",
            "started_at": started_at,
            "ended_at": ended_at,
            "sequence_path": "calib",
        },
        {
            "name": "check",
            "outcome": "FAIL",
            "started_at": started_at,
            "ended_at": ended_at,
            "sequence_path": "calib/dmm",
        },
    ]
    result = client.runs.create(
        serial_number=f"SN-SEQ-{unique}",
        procedure_id=procedure_id,
        part_number=f"PART-SEQ-{unique}",
        started_at=started_at,
        outcome="FAIL",
        ended_at=ended_at,
        phases=phases,
        sequences=sequences,
    )
    assert_create_run_success(result)
    return result.id


def test_sequences_are_stored_as_a_tree_and_read_back_on_get(client: TofuPilot, procedure_id: str):
    """Every call comes back with its path, parent and outcome, and every
    phase names the call it ran in."""
    run_id = _create_run_with_sequences(client, procedure_id, uid())

    run = client.runs.get(id=run_id)
    assert run.sequences is not None
    by_path = {s.path: s for s in run.sequences}
    assert sorted(by_path) == ["calib", "calib/dmm", "final"]
    assert by_path["calib"].parent_path is None
    assert by_path["calib/dmm"].parent_path == "calib"
    assert by_path["calib/dmm"].key == "dmm"
    assert by_path["calib"].outcome == "FAIL"
    assert by_path["final"].outcome == "SKIP"
    assert by_path["final"].started_at is None
    assert by_path["final"].ended_at is None
    assert by_path["calib"].source == "sequences/calib.yaml"

    assert run.phases is not None
    membership = sorted((p.name, p.sequence_path) for p in run.phases)
    assert membership == [
        ("check", "calib"),
        ("check", "calib/dmm"),
        ("warmup", None),
        ("zero", "calib"),
    ]


def test_list_returns_top_level_calls_only_when_asked(client: TofuPilot, procedure_id: str):
    """`include_sequences` adds the level-1 calls to each run of the list;
    without it the field is absent."""
    run_id = _create_run_with_sequences(client, procedure_id, uid())

    with_sequences = client.runs.list(ids=[run_id], include_sequences=True)
    assert len(with_sequences.data) == 1
    sequences = with_sequences.data[0].sequences
    assert sequences is not None
    # Level 1 only: the nested `calib/dmm` is not listed here.
    assert sorted(s.path for s in sequences) == ["calib", "final"]

    plain = client.runs.list(ids=[run_id])
    assert len(plain.data) == 1
    assert plain.data[0].sequences is None


def test_phases_list_carries_the_call_and_filters_by_it(client: TofuPilot, procedure_id: str):
    """`phases.list` exposes `sequence_path`, and `sequence_paths` matches a
    call together with the calls nested in it."""
    run_id = _create_run_with_sequences(client, procedure_id, uid())

    all_phases = client.phases.list(procedure_id=procedure_id, ids=[run_id])
    assert sorted((p.name, p.sequence_path) for p in all_phases.data) == [
        ("check", "calib"),
        ("check", "calib/dmm"),
        ("warmup", None),
        ("zero", "calib"),
    ]

    in_calib = client.phases.list(procedure_id=procedure_id, ids=[run_id], sequence_paths=["calib"])
    assert sorted((p.name, p.sequence_path) for p in in_calib.data) == [
        ("check", "calib"),
        ("check", "calib/dmm"),
        ("zero", "calib"),
    ]

    in_dmm = client.phases.list(
        procedure_id=procedure_id, ids=[run_id], sequence_paths=["calib/dmm"]
    )
    assert [(p.name, p.sequence_path) for p in in_dmm.data] == [("check", "calib/dmm")]


def test_a_nested_call_without_its_parent_is_refused(client: TofuPilot, procedure_id: str):
    """A path with several segments needs its parent in the same array."""
    started_at, ended_at = get_random_test_dates()
    unique = uid()

    with pytest.raises(ErrorBADREQUEST) as exc_info:
        client.runs.create(
            serial_number=f"SN-SEQ-ORPHAN-{unique}",
            procedure_id=procedure_id,
            part_number=f"PART-SEQ-ORPHAN-{unique}",
            started_at=started_at,
            outcome="PASS",
            ended_at=ended_at,
            phases=[
                {
                    "name": "check",
                    "outcome": "PASS",
                    "started_at": started_at,
                    "ended_at": ended_at,
                    "sequence_path": "calib/dmm",
                }
            ],
            sequences=[
                {
                    "path": "calib/dmm",
                    "name": "DMM",
                    "outcome": "PASS",
                    "started_at": started_at,
                    "ended_at": ended_at,
                }
            ],
        )
    assert "calib" in str(exc_info.value)
