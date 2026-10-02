"""Execution identity only: a token is not a lease or external-effect fence."""

from uuid import uuid4


def new_execution_token() -> str:
    """Fresh internal identity for every committed execution claim."""
    return str(uuid4())
