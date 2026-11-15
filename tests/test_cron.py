"""Cron expressions and intervals: parsing, next fire time, daylight saving."""

import json
from datetime import UTC, datetime, timedelta

import pytest

import dewey
from dewey.core.cron import Cron, Every, Trigger, trigger_from_dict

LONDON = "Europe/London"  # 2026: clocks go forward 29 March, back 25 October


def utc(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


def fires(trigger, start: str, count: int) -> list[str]:
    instant, out = utc(start), []
    for _ in range(count):
        instant = trigger.next_after(instant)
        out.append(instant.strftime("%m-%d %H:%M"))
    return out


@pytest.mark.parametrize(
    ("expression", "start", "expected"),
    [
        ("* * * * *", "2026-01-01 00:00", ["01-01 00:01", "01-01 00:02"]),
        ("*/15 * * * *", "2026-01-01 00:50", ["01-01 01:00", "01-01 01:15"]),
        ("5/20 * * * *", "2026-01-01 00:00", ["01-01 00:05", "01-01 00:25", "01-01 00:45"]),
        ("0 9-10 * * *", "2026-01-01 09:30", ["01-01 10:00", "01-02 09:00"]),
        ("0 0 1 jan,JUL *", "2026-02-01 00:00", ["07-01 00:00", "01-01 00:00"]),
        ("0 9 * * mon-fri", "2026-11-13 10:00", ["11-16 09:00"]),  # Friday → Monday
        ("0 0 * * 0", "2026-11-12 00:00", ["11-15 00:00"]),
        ("0 0 * * 7", "2026-11-12 00:00", ["11-15 00:00"]),  # 7 is Sunday too
        ("0 0 31 * *", "2026-01-31 00:00", ["03-31 00:00", "05-31 00:00"]),
        ("0 0 29 2 *", "2026-01-01 00:00", ["02-29 00:00"]),  # 2028
        ("59 23 31 12 *", "2026-12-31 23:59", ["12-31 23:59"]),  # 2027
        ("@daily", "2026-01-01 00:00", ["01-02 00:00"]),
        # Both day fields restricted: either one matching is enough.
        ("0 0 13 * fri", "2026-11-12 00:00", ["11-13 00:00", "11-20 00:00"]),
        ("0 0 10 * mon", "2026-01-09 00:00", ["01-10 00:00", "01-12 00:00"]),
        ("1,10,30 3-5 * * *", "2026-01-01 03:30", ["01-01 04:01", "01-01 04:10"]),
        ("10-50/20 0 * * *", "2026-01-01 00:10", ["01-01 00:30", "01-01 00:50"]),
        ("0 0 1 * *", "2026-12-31 23:59", ["01-01 00:00", "02-01 00:00"]),  # 2027
    ],
)
def test_next_fire_time(expression, start, expected):
    assert fires(dewey.cron(expression), start, len(expected)) == expected


@pytest.mark.parametrize(
    "expression",
    [
        "",
        "* * * *",
        "0 * * * * *",  # seconds are not supported
        "60 * * * *",
        "* 24 * * *",
        "* * 0 * *",
        "* * * 13 *",
        "* * * * 8",
        "*/0 * * * *",
        "5-1 * * * *",
        "1,,2 * * * *",
        "* * L * *",
        "* * 15W * *",
        "* * * * 1#2",
        "? * * * *",
        "a * * * *",
        "0 0 30 2 *",  # a date that never occurs
        "@reboot",
        "-1 * * * *",
        "*/-1 * * * *",
        "*/x * * * *",
        "1//2 * * * *",
        "1/2/3 * * * *",
        "1-2-3 * * * *",
        "1.5 * * * *",
        "* * * xx *",
        "* * * * thur",
        "* * * * mon-fri/0",
        "* * * * 7-0",
        "* * * * ,",
    ],
)
def test_malformed_expressions_are_rejected(expression):
    with pytest.raises(ValueError):
        dewey.cron(expression)


@pytest.mark.parametrize(
    ("expression", "start", "expected"),
    [
        # A stepped star is still a star, as in classic cron: the other day field decides.
        ("0 0 */2 * mon", "2026-01-05 00:00", ["01-19 00:00", "02-09 00:00"]),
        ("0 0 */1 * mon", "2026-01-05 00:00", ["01-12 00:00", "01-19 00:00"]),
        ("0 0 13 * */2", "2026-01-05 00:00", ["01-13 00:00", "06-13 00:00"]),
    ],
)
def test_a_stepped_star_does_not_switch_the_day_fields_to_either_or(expression, start, expected):
    assert fires(dewey.cron(expression), start, len(expected)) == expected


def test_a_leap_day_schedule_skips_the_century_that_has_none():
    leap_day = dewey.cron("0 0 29 feb *")
    assert leap_day.next_after(utc("2025-03-01 00:00")) == utc("2028-02-29 00:00")
    assert leap_day.next_after(utc("2096-02-29 00:00")) == utc("2104-02-29 00:00")  # not 2100


def test_unknown_timezone_and_naive_instants_are_rejected():
    with pytest.raises(ValueError, match="Unknown timezone"):
        dewey.cron("* * * * *", tz="Mars/Olympus")
    with pytest.raises(ValueError, match="timezone-aware"):
        dewey.cron("* * * * *").next_after(datetime(2026, 1, 1))


def test_a_wall_clock_time_follows_its_zone():
    # 03:15 in a zone five and a half hours ahead of UTC.
    assert fires(dewey.cron("15 3 * * *", tz="Asia/Kolkata"), "2026-01-01 00:00", 1) == [
        "01-01 21:45"
    ]
    summer = fires(dewey.cron("15 3 * * *", tz=LONDON), "2026-07-01 00:00", 1)
    winter = fires(dewey.cron("15 3 * * *", tz=LONDON), "2026-12-01 00:00", 1)
    assert (summer, winter) == (["07-01 02:15"], ["12-01 03:15"])


class TestDaylightSaving:
    def test_a_time_that_does_not_exist_fires_once_after_the_gap(self):
        # 01:30 local does not exist on 29 March; 01:00 UTC is the first valid instant.
        assert fires(dewey.cron("30 1 * * *", tz=LONDON), "2026-03-28 00:00", 3) == [
            "03-28 01:30",
            "03-29 01:00",
            "03-30 00:30",
        ]

    def test_a_time_that_occurs_twice_fires_on_the_first_occurrence_only(self):
        assert fires(dewey.cron("30 1 * * *", tz=LONDON), "2026-10-24 00:00", 3) == [
            "10-24 00:30",
            "10-25 00:30",
            "10-26 01:30",
        ]

    def test_asking_between_the_two_occurrences_does_not_fire_the_second(self):
        # 01:10 UTC on 25 October is 01:10 local for the second time.
        assert fires(dewey.cron("30 1 * * *", tz=LONDON), "2026-10-25 01:10", 1) == ["10-26 01:30"]

    def test_several_times_inside_one_gap_collapse_into_one_fire(self):
        assert fires(dewey.cron("0,30 1-2 * * *", tz=LONDON), "2026-03-29 00:00", 3) == [
            "03-29 01:00",
            "03-29 01:30",
            "03-30 00:00",
        ]

    def test_a_half_hour_shift_fires_once_after_its_gap(self):
        # Lord Howe Island moves 30 minutes: 02:15 does not exist on 4 October 2026.
        assert fires(dewey.cron("15 2 * * *", tz="Australia/Lord_Howe"), "2026-10-03 13:00", 2) == [
            "10-03 15:30",
            "10-04 15:15",
        ]

    def test_an_every_hour_schedule_keeps_its_cadence_through_both_changes(self):
        half_hourly = dewey.cron("*/30 * * * *", tz=LONDON)
        for start in ("2026-03-28 23:50", "2026-10-24 23:50"):
            instant, gaps = utc(start), set()
            for _ in range(12):
                following = half_hourly.next_after(instant)
                gaps.add(following - instant if instant.minute % 30 == 0 else None)
                instant = following
            assert gaps - {None} == {timedelta(minutes=30)}


class TestEvery:
    def test_intervals_align_to_the_epoch(self):
        assert fires(dewey.every(minutes=5), "2026-11-13 10:02", 2) == [
            "11-13 10:05",
            "11-13 10:10",
        ]
        assert fires(dewey.every(hours=1, minutes=30), "2026-01-01 00:00", 1) == ["01-01 01:30"]
        # Strictly after: asking at a tick returns the next one.
        assert fires(dewey.every(minutes=1), "2026-01-01 00:01", 1) == ["01-01 00:02"]

    def test_shorter_than_a_minute_is_rejected(self):
        with pytest.raises(ValueError, match="one minute"):
            dewey.every(seconds=59)
        with pytest.raises(TypeError):
            dewey.every(minutes=1.5)  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        ("parts", "seconds"),
        [
            ({"minutes": 1}, 60),
            ({"seconds": 61}, 61),  # need not be a whole number of minutes
            ({"hours": 1, "minutes": 30}, 5400),
            ({"days": 1, "minutes": 1}, 86460),
            ({"minutes": 1, "seconds": 30}, 90),
        ],
    )
    def test_units_add_up(self, parts, seconds):
        assert dewey.every(**parts) == Every(seconds)

    @pytest.mark.parametrize(
        "parts",
        [
            {},
            {"minutes": -1},
            {"seconds": True},
            {"minutes": "1"},
            {"seconds": float("nan")},
            {"seconds": float("inf")},
        ],
    )
    def test_no_or_nonsense_units_are_rejected(self, parts):
        with pytest.raises((ValueError, TypeError)):
            dewey.every(**parts)

    def test_alignment_is_strict_and_holds_before_the_epoch(self):
        interval = dewey.every(seconds=90)
        assert interval.next_after(utc("1970-01-01 00:00")) == utc("1970-01-01 00:01:30")
        assert interval.next_after(utc("1969-12-31 23:59:59")) == utc("1970-01-01 00:00")
        just_before = utc("1970-01-01 00:01:29").replace(microsecond=999999)
        assert interval.next_after(just_before) == utc("1970-01-01 00:01:30")


def test_definitions_round_trip_and_compare_by_meaning():
    for trigger in (dewey.cron("15  3 * * *", tz=LONDON), dewey.every(minutes=5)):
        assert trigger_from_dict(trigger.to_dict()) == trigger
    assert dewey.cron("15  3 * * *") == Cron("15 3 * * *")  # spacing is not identity
    assert dewey.every(minutes=2) == Every(120)
    with pytest.raises(ValueError):
        trigger_from_dict({"kind": "lunar"})


@pytest.mark.parametrize(
    "trigger", [dewey.cron(" 0  1 * jan mon ", tz=LONDON), dewey.every(minutes=3)]
)
def test_a_definition_survives_json_and_fires_in_utc(trigger):
    assert isinstance(trigger, Trigger)
    assert trigger_from_dict(json.loads(json.dumps(trigger.to_dict()))) == trigger
    assert trigger.next_after(utc("2026-01-01 00:00")).tzinfo is UTC
