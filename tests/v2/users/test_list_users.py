"""Test listing users and the current-user route."""

import pytest
from tofupilot.v2 import TofuPilot
from tofupilot.v2.errors import ErrorFORBIDDEN
from tofupilot.v2.models import UserListResponse


class TestListUsers:
    """Test user listing functionality."""

    def test_list_all_users(self, client: TofuPilot, auth_type: str) -> None:
        """Test listing all organization users returns a non-empty list with expected fields."""
        result = client.users.list()

        assert isinstance(result, list)
        assert len(result) > 0

        user = result[0]
        assert isinstance(user, UserListResponse)
        assert len(user.id) > 0
        assert len(user.email) > 0
        assert isinstance(user.banned, bool)

    def test_get_current_user(self, client: TofuPilot, auth_type: str) -> None:
        """GET /v2/users/current returns the authenticated user; a station key is refused."""
        if auth_type == "station":
            # Stations have no current user, the mirror of users on stations.get_current
            with pytest.raises(ErrorFORBIDDEN) as exc_info:
                client.users.get_current()
            assert exc_info.value.status_code == 403
            return

        current_user = client.users.get_current()

        assert len(current_user.id) > 0
        assert len(current_user.email) > 0
        assert isinstance(current_user.banned, bool)

        all_ids = [u.id for u in client.users.list()]
        assert current_user.id in all_ids

    def test_whoami(self, client: TofuPilot, auth_type: str) -> None:
        """GET /v2/whoami names the identity behind either kind of key."""
        me = client.identity.whoami()

        assert me.organization.slug
        if auth_type == "station":
            assert me.type == "station"
            assert me.user is None
            assert me.station is not None
            assert me.station.id == client.stations.get_current().id
        else:
            assert me.type == "user"
            assert me.station is None
            assert me.user is not None
            assert me.user.id == client.users.get_current().id
