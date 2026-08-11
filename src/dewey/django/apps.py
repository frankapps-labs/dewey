"""Django AppConfig for dewey."""

from django.apps import AppConfig


class DeweyConfig(AppConfig):
    name = "dewey.django"
    label = "dewey"
    verbose_name = "Dewey"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self) -> None:
        # Import registers lightweight checks; no database access occurs here.
        from dewey.django import checks  # noqa: F401
