"""Test custom metadata on units."""

import uuid
import pytest
from tofupilot.v2 import TofuPilot
from tofupilot.v2.errors import APIError, ErrorBADREQUEST


def _make_unit(client: TofuPilot, timestamp: str) -> str:
    """Create a part + revision + unit, return the serial_number.

    parts.create auto-creates a default revision 'A', so use a non-default
    revision number to avoid conflicts when chaining tests.
    """
    suffix = uuid.uuid4().hex[:6]
    part_number = f"META-PART-{timestamp}-{suffix}"
    rev = f"R-{suffix}"
    client.parts.create(number=part_number, name=f"Metadata test part {timestamp}")
    client.parts.revisions.create(part_number=part_number, number=rev)
    serial = f"META-UNIT-{uuid.uuid4().hex[:8]}"
    client.units.create(
        serial_number=serial,
        part_number=part_number,
        revision_number=rev,
    )
    return serial


class TestUnitMetadata:

    def test_create_unit_with_metadata(self, client: TofuPilot, timestamp) -> None:
        """Metadata dict survives round-trip with native types."""
        suffix = uuid.uuid4().hex[:6]
        part_number = f"META-PART-{timestamp}-{suffix}"
        rev = f"R-{suffix}"
        client.parts.create(number=part_number, name="Metadata create test")
        client.parts.revisions.create(part_number=part_number, number=rev)
        serial = f"META-CREATE-{uuid.uuid4().hex[:8]}"

        client.units.create(
            serial_number=serial,
            part_number=part_number,
            revision_number=rev,
            metadata={
                "vendor": "Acme",
                "lot_size": 500,
                "qualified": True,
            },
        )

        unit = client.units.get(serial_number=serial)
        assert unit.metadata is not None
        assert unit.metadata["vendor"] == "Acme"
        assert unit.metadata["lot_size"] == 500
        assert unit.metadata["qualified"] is True

    def test_update_metadata_patch_preserves_other_keys(
        self, client: TofuPilot, timestamp
    ) -> None:
        """PATCH semantics: keys not present in payload stay on the entity."""
        serial = _make_unit(client, timestamp)
        client.units.update(
            serial_number=serial,
            metadata={"a": "first", "b": "second"},
        )
        client.units.update(
            serial_number=serial,
            metadata={"a": "updated"},
        )
        unit = client.units.get(serial_number=serial)
        assert unit.metadata == {"a": "updated", "b": "second"}

    def test_update_metadata_null_deletes_key(
        self, client: TofuPilot, timestamp
    ) -> None:
        """Passing null as a value deletes that key."""
        serial = _make_unit(client, timestamp)
        client.units.update(
            serial_number=serial,
            metadata={"keep": "me", "drop": "later"},
        )
        client.units.update(
            serial_number=serial,
            metadata={"drop": None},
        )
        unit = client.units.get(serial_number=serial)
        assert unit.metadata == {"keep": "me"}

    def test_update_metadata_null_diff_replaces_full_set(
        self, client: TofuPilot, timestamp
    ) -> None:
        """Caller can mimic 'replace' by sending null for keys to drop."""
        serial = _make_unit(client, timestamp)
        client.units.update(
            serial_number=serial,
            metadata={"a": 1, "b": 2, "c": 3},
        )
        client.units.update(
            serial_number=serial,
            metadata={"only": "this", "a": None, "b": None, "c": None},
        )
        unit = client.units.get(serial_number=serial)
        assert unit.metadata == {"only": "this"}

    def test_update_metadata_type_change_on_existing_key(
        self, client: TofuPilot, timestamp
    ) -> None:
        """Server allows changing the type of an existing key."""
        serial = _make_unit(client, timestamp)
        client.units.update(serial_number=serial, metadata={"flag": "yes"})
        client.units.update(serial_number=serial, metadata={"flag": True})
        unit = client.units.get(serial_number=serial)
        assert unit.metadata is not None
        assert unit.metadata["flag"] is True

    def test_metadata_invalid_key_chars_rejected(
        self, client: TofuPilot, timestamp
    ) -> None:
        """Keys outside [a-zA-Z0-9_.:+-] are rejected at the API boundary."""
        serial = _make_unit(client, timestamp)
        with pytest.raises((ErrorBADREQUEST, APIError)):
            client.units.update(
                serial_number=serial,
                metadata={"has space": "nope"},
            )

    def test_metadata_max_50_keys(self, client: TofuPilot, timestamp) -> None:
        """50-key cap is enforced before any DB write."""
        serial = _make_unit(client, timestamp)
        too_many = {f"k{i}": i for i in range(51)}
        with pytest.raises((ErrorBADREQUEST, APIError)):
            client.units.update(serial_number=serial, metadata=too_many)
        # Confirm nothing was written
        unit = client.units.get(serial_number=serial)
        assert not unit.metadata

    @pytest.mark.skip(
        reason="Query param encoding mismatch: SDK form-encodes nested dict as "
        "metadata[k][in]=v but server expects JSON-encoded string. Filter works "
        "via internal caller; needs wider tRPC-rest query serialization fix."
    )
    def test_list_units_metadata_filter_string_in(
        self, client: TofuPilot, timestamp
    ) -> None:
        """list metadata={key:{in:[...]}} filter returns matching units only."""
        s_match = _make_unit(client, timestamp)
        s_other = _make_unit(client, timestamp)
        marker = f"vendor-{uuid.uuid4().hex[:6]}"
        client.units.update(serial_number=s_match, metadata={"vendor": marker})
        client.units.update(serial_number=s_other, metadata={"vendor": "other"})

        result = client.units.list(
            metadata={"vendor": {"in": [marker]}},
            include_metadata=True,
        )
        ids = [u.serial_number for u in result.data]
        assert s_match in ids
        assert s_other not in ids

    @pytest.mark.skip(reason="Same query encoding mismatch as filter_string_in.")
    def test_list_units_metadata_filter_number_range(
        self, client: TofuPilot, timestamp
    ) -> None:
        """Number filter with gte/lte selects the right rows."""
        s_in = _make_unit(client, timestamp)
        s_out = _make_unit(client, timestamp)
        # Use a unique key so tests don't collide on shared dev DB
        key = f"qty_{uuid.uuid4().hex[:6]}"
        client.units.update(serial_number=s_in, metadata={key: 250})
        client.units.update(serial_number=s_out, metadata={key: 999})

        result = client.units.list(
            metadata={key: {"gte": 100, "lte": 500}},
            include_metadata=True,
        )
        ids = [u.serial_number for u in result.data]
        assert s_in in ids
        assert s_out not in ids
