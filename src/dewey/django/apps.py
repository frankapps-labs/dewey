"""Django AppConfig for dewey."""

from django.apps import AppConfig


class DeweyConfig(AppConfig):
    name = "dewey.django"
    label = "dewey"
    verbose_name = "Dewey"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self) -> None:
        # Import notification models so Django discovers them
        import dewey.django.notification_models  # noqa: F401
