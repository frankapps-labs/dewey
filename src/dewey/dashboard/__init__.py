"""Framework-neutral dashboard policies, read contract and packaged browser assets.

Adapters own authentication, authorization, database access and HTTP responses.
Nothing in this module imports Django or SQLAlchemy.
"""

from importlib.resources import files

from dewey.dashboard.contract import DashboardProvider, TaskFilter
from dewey.dashboard.operations import Diagnostics, FailureCategory, RetryDecision

_ASSETS = {
    "dashboard.js": "text/javascript",
    "dashboard.css": "text/css",
    "chart.js": "text/javascript",
    "chart.LICENSE.txt": "text/plain",
}


def asset(name: str) -> tuple[bytes, str]:
    """Read an allowlisted asset for an authenticated host's static-file route."""
    if name not in _ASSETS:
        raise ValueError("Unknown dashboard asset")
    return files("dewey.dashboard").joinpath("assets", name).read_bytes(), _ASSETS[name]
