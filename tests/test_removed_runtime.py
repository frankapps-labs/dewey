"""The broker and dispatcher paths removed in 0.6 are gone, not merely unused."""

from importlib.util import find_spec

import pytest

import dewey


@pytest.mark.parametrize(
    "module",
    [
        "dewey.adapters",
        "dewey.contrib",
        "dewey.dispatcher",
        "dewey.listen_sync",
        "dewey.django.dispatch",
        "dewey.django.listen",
        "dewey.django.sweep",
        "dewey.sqlalchemy.dispatch",
        "dewey.sqlalchemy.listen",
        "dewey.sqlalchemy.sweep",
        "dewey.sqlalchemy.async_sweep",
        "dewey.django.management.commands.dewey_dispatcher",
    ],
)
def test_a_removed_module_has_no_code_left(module):
    spec = find_spec(module)
    # A stale empty directory in a working tree is a namespace with no origin: nothing to run.
    assert spec is None or spec.origin is None


def test_the_public_names_of_the_removed_paths_are_gone():
    for name in ("Dispatcher", "TaskAdapter", "sweep_stuck", "sweep_retries"):
        assert not hasattr(dewey, name)
