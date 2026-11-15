import pytest
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission


@pytest.fixture(autouse=True)
def cleanup():
    """Use pytest-django transactions, not the parent's SQLAlchemy schema reset."""
    yield


@pytest.fixture
def viewer(db, client):
    user = get_user_model().objects.create_user(username="test-dashboard-viewer", is_staff=True)
    user.user_permissions.add(Permission.objects.get(codename="view_taskentry"))
    client.force_login(user)
    return client
