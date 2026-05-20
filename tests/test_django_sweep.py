"""Tests for Django sweep — re-enqueue failed, unstick stuck."""

# ruff: noqa: E402 — django.setup() must run before model imports

import os
from datetime import timedelta

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.django_settings")
django.setup()

from django.utils import timezone

from dewey.core.states import TaskStatus
from dewey.django.executor import create_task
from dewey.django.models import TaskEntry
from dewey.django.sweep import sweep, sweep_failed, sweep_stuck


@pytest.fixture(autouse=True)
def clean_db():
    yield
    TaskEntry.objects.all().delete()


@pytest.mark.django_db(transaction=True)
class TestSweepFailed:
    def test_re_enqueues_failed_tasks_past_process_after(self):
        task = create_task(task_type="test.task")
        TaskEntry.objects.filter(id=task.id).update(
            status=TaskStatus.FAILED.value,
            process_after=timezone.now() - timedelta(minutes=5),
            attempts=1,
        )

        ids = sweep_failed()
        assert task.id in ids

        updated = TaskEntry.objects.get(id=task.id)
        assert updated.status == TaskStatus.PENDING.value

    def test_skips_failed_tasks_not_yet_ready(self):
        task = create_task(task_type="test.task")
        TaskEntry.objects.filter(id=task.id).update(
            status=TaskStatus.FAILED.value,
            process_after=timezone.now() + timedelta(hours=1),
            attempts=1,
        )

        ids = sweep_failed()
        assert task.id not in ids

        updated = TaskEntry.objects.get(id=task.id)
        assert updated.status == TaskStatus.FAILED.value

    def test_dead_letters_exhausted_failed_tasks(self):
        task = create_task(task_type="test.task", max_attempts=1)
        TaskEntry.objects.filter(id=task.id).update(
            status=TaskStatus.FAILED.value,
            process_after=timezone.now() - timedelta(minutes=5),
            attempts=1,
        )

        ids = sweep_failed()
        assert task.id not in ids

        updated = TaskEntry.objects.get(id=task.id)
        assert updated.status == TaskStatus.DEAD.value

    def test_returns_empty_when_no_failed(self):
        create_task(task_type="test.task")  # pending, not failed
        ids = sweep_failed()
        assert ids == []


@pytest.mark.django_db(transaction=True)
class TestSweepStuck:
    def test_unsticks_processing_tasks(self):
        task = create_task(task_type="test.task")
        TaskEntry.objects.filter(id=task.id).update(
            status=TaskStatus.PROCESSING.value,
            started_at=timezone.now() - timedelta(minutes=30),
        )

        ids = sweep_stuck(stuck_threshold_minutes=10)
        assert task.id in ids

        updated = TaskEntry.objects.get(id=task.id)
        assert updated.status == TaskStatus.PENDING.value

    def test_skips_recently_started(self):
        task = create_task(task_type="test.task")
        TaskEntry.objects.filter(id=task.id).update(
            status=TaskStatus.PROCESSING.value,
            started_at=timezone.now() - timedelta(minutes=2),
        )

        ids = sweep_stuck(stuck_threshold_minutes=10)
        assert task.id not in ids

    def test_dead_letters_exhausted_stuck_tasks(self):
        task = create_task(task_type="test.task", max_attempts=1)
        TaskEntry.objects.filter(id=task.id).update(
            status=TaskStatus.PROCESSING.value,
            started_at=timezone.now() - timedelta(minutes=30),
            attempts=1,
        )

        ids = sweep_stuck(stuck_threshold_minutes=10)
        assert task.id not in ids

        updated = TaskEntry.objects.get(id=task.id)
        assert updated.status == TaskStatus.DEAD.value


@pytest.mark.django_db(transaction=True)
class TestSweepCombined:
    def test_sweep_runs_both(self):
        failed = create_task(task_type="test.fail")
        stuck = create_task(task_type="test.stuck")

        TaskEntry.objects.filter(id=failed.id).update(
            status=TaskStatus.FAILED.value,
            process_after=timezone.now() - timedelta(minutes=5),
            attempts=1,
        )
        TaskEntry.objects.filter(id=stuck.id).update(
            status=TaskStatus.PROCESSING.value,
            started_at=timezone.now() - timedelta(minutes=30),
        )

        result = sweep()
        assert failed.id in result["failed"]
        assert stuck.id in result["stuck"]
