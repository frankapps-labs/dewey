"""Minimal Django settings for dewey tests."""

import os

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": "dewey_test",
        "USER": os.environ.get("PGUSER", "postgres"),
        "PASSWORD": os.environ.get("PGPASSWORD", "postgres"),
        "HOST": os.environ.get("PGHOST", "localhost"),
        "PORT": os.environ.get("PGPORT", "5432"),
    },
    # A physically separate database for the documented dedicated-alias/router
    # deployment (docs/getting-started.md, "Sharing a database with your
    # application"). Separate NAME on purpose: a second alias onto the same
    # database could not prove that reads, locks and writes stay on one alias.
    "second": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": "dewey_test_second",
        "USER": os.environ.get("PGUSER", "postgres"),
        "PASSWORD": os.environ.get("PGPASSWORD", "postgres"),
        "HOST": os.environ.get("PGHOST", "localhost"),
        "PORT": os.environ.get("PGPORT", "5432"),
    },
}

INSTALLED_APPS = [
    "dewey.django",
]

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

USE_TZ = True
TIME_ZONE = "UTC"
