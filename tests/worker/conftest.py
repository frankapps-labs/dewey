"""Process-core tests do not use a database or the root schema-reset fixture."""

import pytest


@pytest.fixture(autouse=True)
def cleanup():
    """Override root DB cleanup: this suite exercises storage-free supervision."""
    yield
