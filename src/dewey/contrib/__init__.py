"""Framework integrations that wire Dewey into a host project.

Nothing in core ``dewey`` imports this package, and this package imports no
framework at import time — ``dewey.contrib`` is importable in any environment.
Each submodule declares its own requirements and fails with an actionable error
when they are missing. Import the one you use, e.g.
``dewey.contrib.django_huey``.
"""
