"""Schedule declarations and the pure rules for which ticks are due."""

from datetime import UTC, datetime, timedelta

import pytest

import dewey
from dewey.errors import DuplicateTaskTypeError, SerializationError
from dewey.scheduling import (
    CATCH_UP_LIMIT,
    SCAN_LIMIT,
    DuplicateScheduleError,
    definition_hash,
    plan_ticks,
    tick_key,
)


@pytest.fixture(autouse=True)
def clean_registries():
    yield
    dewey.schedules.clear()
    dewey.registry.clear()


def at(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


class TestDeclaration:
    def test_the_decorator_declares_a_schedule_named_after_the_task(self):
        @dewey.task("reports.daily", queue="reports", schedule=dewey.cron("15 3 * * *"))
        def daily():
            pass

        declared = dewey.schedules.get("reports.daily")
        assert declared is not None and declared.task_type == "reports.daily"
        assert (declared.catch_up, declared.overlap, declared.expire_at_next_tick) == (
            "once",
            "skip",
            True,
        )
        # The task's routing is resolved into the definition.
        assert declared.definition()["queue"] == "reports"

    def test_a_schedule_has_its_own_name_and_arguments(self):
        dewey.schedule(
            "poll-inventory",
            task="inventory.poll",
            when=dewey.every(minutes=1),
            args=("eu",),
            kwargs={"full": False},
            queue="polls",
            priority=3,
        )
        definition = dewey.schedules.get("poll-inventory").definition()
        assert definition["args"] == ["eu"] and definition["kwargs"] == {"full": False}
        assert (definition["queue"], definition["priority"]) == ("polls", 3)

    def test_every_period_matters_means_no_overlap_skip_and_no_expiry(self):
        billing = dewey.schedule(
            "billing", task="billing.run", when=dewey.cron("0 * * * *"), catch_up="all"
        )
        assert (billing.overlap, billing.expire_at_next_tick) == ("allow", False)
        with pytest.raises(ValueError, match="every tick"):
            dewey.schedule(
                "b2",
                task="billing.run",
                when=dewey.cron("0 * * * *"),
                catch_up="all",
                overlap="skip",
            )
        spelled_out = dewey.schedule(
            "b3",
            task="billing.run",
            when=dewey.cron("0 * * * *"),
            catch_up="all",
            overlap="allow",
            expire="next_tick",
        )
        assert spelled_out.expire_at_next_tick is False  # a tick that must run cannot expire

    def test_redeclaring_the_same_schedule_is_fine_but_a_different_one_is_not(self):
        for _ in range(2):  # a re-imported module
            dewey.schedule("nightly", task="clean.up", when=dewey.cron("0 2 * * *"))
        with pytest.raises(DuplicateScheduleError):
            dewey.schedule("nightly", task="clean.up", when=dewey.cron("0 3 * * *"))

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"name": ""},
            {"name": "x" * 101},
            {"name": "a\0b"},
            {"task": ""},
            {"catch_up": "sometimes"},
            {"overlap": "queue"},
            {"expire": "later"},
        ],
    )
    def test_invalid_declarations_fail_at_import_time(self, kwargs):
        base = {"name": "ok", "task": "t", "when": dewey.every(minutes=1)}
        base.update(kwargs)
        name = base.pop("name")
        with pytest.raises(ValueError):
            dewey.schedule(name, **base)

    def test_when_must_be_a_trigger_and_arguments_json_safe(self):
        with pytest.raises(TypeError):
            dewey.schedule("bad", task="t", when="* * * * *")  # type: ignore[arg-type]
        with pytest.raises(SerializationError):
            dewey.schedule("bad", task="t", when=dewey.every(minutes=1), args=(object(),))
        assert dewey.schedules.get("bad") is None

    def test_the_decorator_returns_the_handler_unchanged(self):
        def handler():
            return "ok"

        decorated = dewey.task("clean.daily", schedule=dewey.cron("0 3 * * *"))(handler)
        assert decorated is handler and decorated() == "ok"
        assert dewey.registry.get("clean.daily").handler is handler

    def test_a_rejected_decorator_leaves_nothing_registered(self):
        dewey.schedule("poll", task="another.task", when=dewey.every(minutes=1))
        with pytest.raises(DuplicateScheduleError):
            dewey.task("poll", schedule=dewey.every(minutes=5))(lambda: None)
        assert dewey.registry.get("poll") is None
        dewey.task("clean.up")(lambda: None)
        with pytest.raises(DuplicateTaskTypeError):
            dewey.task("clean.up", schedule=dewey.every(minutes=1))(lambda: None)
        assert dewey.schedules.get("clean.up") is None

    def test_the_registry_lists_by_name(self):
        later = dewey.schedule("b", task="t", when=dewey.every(minutes=1))
        earlier = dewey.schedule("a", task="t", when=dewey.every(minutes=1))
        assert dewey.schedules.all() == (earlier, later)
        assert dewey.schedules.get("a") is earlier and dewey.schedules.get("missing") is None
        dewey.schedules.unregister("a")
        assert dewey.schedules.all() == (later,)

    def test_a_name_may_use_the_full_length(self):
        assert len(dewey.schedule("x" * 100, task="t", when=dewey.every(minutes=1)).name) == 100

    @pytest.mark.parametrize(
        "routing",
        [
            {"queue": ""},
            {"queue": "q" * 51},
            {"queue": 42},
            {"task": "t" * 101},
            {"priority": True},
            {"priority": 1.5},
            {"priority": 2**31},
        ],
    )
    def test_routing_a_tick_could_not_be_stored_with_fails_at_import_time(self, routing):
        arguments = {"task": "t", "when": dewey.every(minutes=1), **routing}
        with pytest.raises((ValueError, TypeError)):
            dewey.schedule("bad", **arguments)
        assert dewey.schedules.get("bad") is None

    @pytest.mark.parametrize(
        "arguments",
        [
            {"args": "abc"},  # a string is not a list of arguments
            {"kwargs": []},
            {"kwargs": {1: "a"}},
            {"args": [float("nan")]},
            {"kwargs": {"bad": {1, 2}}},
        ],
    )
    def test_arguments_are_a_json_safe_sequence_and_mapping(self, arguments):
        with pytest.raises(SerializationError):
            dewey.schedule("bad", task="t", when=dewey.every(minutes=1), **arguments)
        assert dewey.schedules.get("bad") is None

    def test_a_declaration_does_not_follow_later_changes_to_its_arguments(self):
        args, kwargs = [[1]], {"nested": [2]}
        declared = dewey.schedule(
            "poll", task="t", when=dewey.every(minutes=1), args=args, kwargs=kwargs
        )
        before = definition_hash(declared.definition())
        args[0].append(3)
        kwargs["nested"].append(4)
        assert declared.definition()["args"] == [[1]]
        assert declared.definition()["kwargs"] == {"nested": [2]}
        assert definition_hash(declared.definition()) == before

    def test_the_hash_tracks_the_definition_only(self):
        first = dewey.schedule("a", task="t", when=dewey.cron("0 2 * * *"), args=(1,))
        same = dewey.schedule("b", task="t", when=dewey.cron("0  2 * * *"), args=(1,))
        moved = dewey.schedule("c", task="t", when=dewey.cron("0 3 * * *"), args=(1,))
        assert definition_hash(first.definition()) == definition_hash(same.definition())
        assert definition_hash(first.definition()) != definition_hash(moved.definition())


class TestDefinitionHash:
    base = {"task": "inventory.poll", "when": dewey.every(minutes=1)}

    def declare(self, name: str, **changes) -> str:
        return definition_hash(dewey.schedule(name, **{**self.base, **changes}).definition())

    @pytest.mark.parametrize(
        "change",
        [
            {"task": "other.task"},
            {"when": dewey.every(minutes=2)},
            {"when": dewey.cron("* * * * *")},
            {"when": dewey.cron("* * * * *", tz="Europe/London")},
            {"args": [1]},
            {"kwargs": {"a": 1}},
            {"catch_up": "skip"},
            {"catch_up": "all"},
            {"overlap": "allow"},
            {"expire": None},
            {"queue": "batch"},
            {"priority": 1},
        ],
    )
    def test_every_part_of_a_definition_changes_the_hash(self, change):
        assert self.declare("changed", **change) != self.declare("original")
        if "when" in change:  # the zone is part of when a schedule fires
            assert self.declare("utc", when=dewey.cron("* * * * *")) != self.declare(
                "london", when=dewey.cron("* * * * *", tz="Europe/London")
            )

    def test_keyword_order_and_the_name_are_not_part_of_it(self):
        assert self.declare("a", kwargs={"b": 1, "a": 2}) == self.declare(
            "b", kwargs={"a": 2, "b": 1}
        )

    def test_the_hash_is_stable_across_releases(self):
        # Changing this value makes every applied schedule look "changed" after an upgrade.
        assert self.declare("poll") == (
            "537e6eae6620642535dfffd229ae6ac3d699d2208fd3400d0701064067592359"
        )

    def test_the_tasks_policy_is_resolved_into_the_definition(self):
        dewey.task("inventory.poll", queue="polls", priority=8, max_attempts=3)(lambda: None)
        definition = dewey.schedule("poll", **self.base).definition()
        assert (definition["queue"], definition["priority"], definition["max_attempts"]) == (
            "polls",
            8,
            3,
        )


class TestDueTicks:
    minutely = dewey.every(minutes=1)
    hourly = dewey.cron("0 * * * *")

    def test_nothing_due_keeps_the_cursor(self):
        plan = plan_ticks(
            self.hourly, at("2026-01-01 03:00"), at("2026-01-01 03:59"), catch_up="once"
        )
        assert (plan.create, plan.missed, plan.cursor) == ((), 0, at("2026-01-01 03:00"))
        assert plan.next_due == at("2026-01-01 04:00")

    def test_a_tick_on_time_is_created_under_every_policy(self):
        for catch_up in ("skip", "once", "all"):
            plan = plan_ticks(
                self.hourly, at("2026-01-01 03:00"), at("2026-01-01 04:00:05"), catch_up=catch_up
            )
            assert plan.create == (at("2026-01-01 04:00"),) and plan.missed == 0
            assert plan.cursor == at("2026-01-01 04:00")

    def test_a_two_hour_outage_under_each_policy(self):
        down, up = at("2026-01-01 02:00"), at("2026-01-01 04:00:30")
        # A one-minute poll: 120 ticks came due.
        once = plan_ticks(self.minutely, down, up, catch_up="once")
        assert once.create == (at("2026-01-01 04:00"),) and once.missed == 119
        skip = plan_ticks(self.minutely, down, up, catch_up="skip")
        assert skip.create == (at("2026-01-01 04:00"),)  # still fresh: this is not catch-up
        # A daily job at 03:15 is 45 minutes late.
        daily = dewey.cron("15 3 * * *")
        late = at("2026-01-01 04:00")
        assert plan_ticks(daily, down, late, catch_up="once").create == (at("2026-01-01 03:15"),)
        skipped = plan_ticks(daily, down, late, catch_up="skip")
        assert (skipped.create, skipped.missed) == ((), 1)
        assert skipped.cursor == at("2026-01-01 03:15")  # never reconsidered
        # An hourly billing run owes both periods, oldest first.
        owed = plan_ticks(self.hourly, down, up, catch_up="all")
        assert owed.create == (at("2026-01-01 03:00"), at("2026-01-01 04:00")) and owed.missed == 0

    def test_all_is_capped_at_the_most_recent_ticks(self):
        plan = plan_ticks(
            self.minutely, at("2026-01-01 00:00"), at("2026-01-01 05:00"), catch_up="all"
        )
        assert len(plan.create) == CATCH_UP_LIMIT and plan.missed == 300 - CATCH_UP_LIMIT
        assert plan.create[-1] == at("2026-01-01 05:00")

    @pytest.mark.parametrize("owed", [0, 1, 99, 100, 101])
    def test_the_cap_applies_only_past_its_limit(self, owed):
        start = at("2026-01-01 00:00")
        plan = plan_ticks(self.minutely, start, start + timedelta(minutes=owed), catch_up="all")
        assert len(plan.create) == min(owed, CATCH_UP_LIMIT)
        assert plan.missed == max(0, owed - CATCH_UP_LIMIT)
        assert plan.cursor == start + timedelta(minutes=owed)

    def test_a_very_long_backlog_is_worked_off_in_bounded_passes(self):
        cursor, now, missed, passes = at("2026-01-01 00:00"), at("2026-01-03 00:00"), 0, 0
        while True:
            plan = plan_ticks(self.minutely, cursor, now, catch_up="once")
            cursor, missed, passes = plan.cursor, missed + plan.missed, passes + 1
            if not plan.more:
                break
            assert plan.create == () and plan.missed == SCAN_LIMIT - CATCH_UP_LIMIT
        assert plan.create == (now,) and missed == 2 * 24 * 60 - 1 and passes == 4

    @pytest.mark.parametrize("owed", [SCAN_LIMIT + 1, SCAN_LIMIT + 50, 3 * SCAN_LIMIT])
    def test_running_every_tick_keeps_the_newest_hundred_of_any_backlog(self, owed):
        cursor = at("2026-01-01 00:00")
        now = cursor + timedelta(minutes=owed)
        missed = 0
        while True:
            plan = plan_ticks(self.minutely, cursor, now, catch_up="all")
            cursor, missed = plan.cursor, missed + plan.missed
            if not plan.more:
                break
        newest = tuple(now - timedelta(minutes=back) for back in reversed(range(CATCH_UP_LIMIT)))
        assert plan.create == newest and missed == owed - CATCH_UP_LIMIT and cursor == now


def test_a_tick_key_is_unique_per_schedule_and_instant():
    tick = datetime(2026, 3, 29, 1, 0, tzinfo=UTC)
    assert tick_key("reports.daily", tick) == "schedule:reports.daily:2026-03-29T01:00:00Z"
    assert tick_key("reports.daily", tick) != tick_key("reports.daily", tick + timedelta(minutes=1))
