"""Dashboard with one explicitly authorized, diagnosed retry command; no CRUD."""

from django.apps import apps
from django.conf import settings
from django.contrib import admin
from django.core.exceptions import PermissionDenied
from django.http import HttpResponse, JsonResponse
from django.middleware.csrf import get_token
from django.template.response import TemplateResponse
from django.urls import path, reverse
from django.views.decorators.csrf import csrf_protect

from dewey.dashboard import asset
from dewey.dashboard.contract import TaskFilter
from dewey.django.dashboard import DjangoDashboardProvider
from dewey.django.models import TaskEntry
from dewey.django.operations import RetryRejected, retry_task_once
from dewey.django.retention import RetentionRejected, retention_state, update_retention


@admin.register(TaskEntry)
class TaskEntryAdmin(admin.ModelAdmin):
    actions = None

    def has_view_permission(self, request, obj=None):
        return request.user.has_perm("dewey.view_taskentry")

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def get_urls(self):
        prefix = "dewey_taskentry_dashboard"
        wrapped = self.admin_site.admin_view
        return [
            path("", wrapped(self.dashboard), name="dewey_taskentry_changelist"),
            path("api/task/<str:task_id>/", wrapped(self.task_detail), name=prefix + "_detail"),
            path(
                "api/task/<str:task_id>/retry/",
                wrapped(csrf_protect(self.retry)),
                name=prefix + "_retry",
            ),
            path(
                "api/retention/configure/",
                wrapped(csrf_protect(self.configure_retention)),
                name=prefix + "_retention_configure",
            ),
            path("api/<str:section>/", wrapped(self.api), name=prefix + "_api"),
            path("assets/<str:name>", wrapped(self.asset_view), name=prefix + "_asset"),
            # Deliberately no inherited add/change/delete/action/history routes.
        ]

    def read_access(self, request):
        if request.method != "GET":
            return HttpResponse(status=405, headers={"Allow": "GET"})
        if not self.has_view_permission(request):
            raise PermissionDenied
        return None

    def dashboard(self, request):
        denied = self.read_access(request)
        if denied is not None:
            return denied
        namespace = self.admin_site.name
        api = reverse(namespace + ":dewey_taskentry_dashboard_api", args=["overview"])
        return TemplateResponse(
            request,
            "dewey/dashboard.html",
            {
                **self.admin_site.each_context(request),
                "title": "Dewey",
                "opts": self.model._meta,
                "api_base": api.removesuffix("overview/"),
                "theme_mode": "class" if apps.is_installed("unfold") else "host",
                "csrf_token_value": get_token(request),
                "css_url": reverse(
                    namespace + ":dewey_taskentry_dashboard_asset", args=["dashboard.css"]
                ),
                "js_url": reverse(
                    namespace + ":dewey_taskentry_dashboard_asset", args=["dashboard.js"]
                ),
            },
        )

    def api(self, request, section):
        denied = self.read_access(request)
        if denied is not None:
            return denied
        if section not in ("overview", "tasks", "retention"):
            return JsonResponse({"error": "Not found"}, status=404)
        if section == "retention":
            try:
                raw_days = request.GET.get("days")
                preview_days = int(raw_days) if raw_days is not None else None
                data = retention_state(
                    using=DjangoDashboardProvider().using, preview_days=preview_days
                )
            except (ValueError, TypeError):
                return JsonResponse({"error": "Invalid retention preview."}, status=400)
            data["can_manage"] = request.user.has_perm("dewey.manage_task_retention")
            return JsonResponse(data)
        try:
            filters = TaskFilter.parse(request.GET)
        except ValueError:
            return JsonResponse({"error": "Invalid dashboard filters"}, status=400)
        provider = DjangoDashboardProvider()
        return JsonResponse(getattr(provider, section)(filters))

    def task_detail(self, request, task_id):
        denied = self.read_access(request)
        if denied is not None:
            return denied
        data = DjangoDashboardProvider().detail(task_id)
        if data and not request.user.has_perm("dewey.retry_taskentry"):
            data["task"]["retry"] = {
                "allowed": False,
                "explanation": "Separate retry permission is required.",
            }
        return JsonResponse(
            data if data is not None else {"error": "Not found"}, status=200 if data else 404
        )

    def retry(self, request, task_id):
        if request.method != "POST":
            return HttpResponse(status=405, headers={"Allow": "POST"})
        if not self.has_view_permission(request) or not request.user.has_perm(
            "dewey.retry_taskentry"
        ):
            raise PermissionDenied
        if request.POST.get("confirm") != "yes":
            return JsonResponse({"error": "Explicit confirmation is required."}, status=400)
        try:
            attempt = int(request.POST.get("attempt", ""))
            if attempt < 0:
                raise ValueError
        except (TypeError, ValueError):
            return JsonResponse({"error": "Invalid expected attempt."}, status=400)
        try:
            data = retry_task_once(
                task_id,
                event_id=request.POST.get("event_id", ""),
                attempt=attempt,
                actor=str(request.user.pk),
                reason=request.POST.get("reason", ""),
                using=getattr(settings, "DEWEY_DASHBOARD_DATABASE_ALIAS", None),
            )
        except RetryRejected as exc:
            return JsonResponse({"error": str(exc)}, status=409)
        return JsonResponse(data)

    def configure_retention(self, request):
        if request.method != "POST":
            return HttpResponse(status=405, headers={"Allow": "POST"})
        if not self.has_view_permission(request) or not request.user.has_perm(
            "dewey.manage_task_retention"
        ):
            raise PermissionDenied
        if request.POST.get("confirm") != "yes":
            return JsonResponse({"error": "Explicit confirmation is required."}, status=400)
        try:
            days = int(request.POST.get("days", ""))
            revision = int(request.POST.get("revision", ""))
            enabled = request.POST.get("enabled")
            if not 1 <= days <= 3650 or revision < 0 or enabled not in ("yes", "no"):
                raise ValueError
        except (ValueError, TypeError):
            return JsonResponse({"error": "Invalid retention settings."}, status=400)
        try:
            data = update_retention(
                days=days,
                enabled=enabled == "yes",
                revision=revision,
                actor=str(request.user.pk),
                reason=request.POST.get("reason", ""),
                using=getattr(settings, "DEWEY_DASHBOARD_DATABASE_ALIAS", None),
            )
        except RetentionRejected as exc:
            return JsonResponse({"error": str(exc)}, status=409)
        return JsonResponse(data)

    def asset_view(self, request, name):
        denied = self.read_access(request)
        if denied is not None:
            return denied
        try:
            body, mime = asset(name)
        except ValueError:
            return HttpResponse(status=404)
        return HttpResponse(body, content_type=mime)
