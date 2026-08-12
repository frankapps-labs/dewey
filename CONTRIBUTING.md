# Contributing

Thanks for considering it. Dewey is small on purpose, so the most useful contributions are
usually bug reports with a reproduction, and patches that make an existing guarantee more
solid rather than adding a new one.

## Getting set up

```bash
git clone https://github.com/frankapps-labs/dewey
cd dewey
make install          # uv sync with all extras
make up               # Postgres + Redis in Docker, on offset ports
make test-integration # the suite, against those containers
make down
```

If you already run Postgres locally, `make test` works too — point
`DEWEY_TEST_DATABASE_URL` at it. The suite needs a real PostgreSQL: `FOR UPDATE SKIP
LOCKED`, partial indexes, LISTEN/NOTIFY and committed claims cannot be proven against
SQLite or a mock, and tests that pretend otherwise are worse than no tests.

## Before opening a pull request

```bash
make lint typecheck format-check test
```

CI runs the same gates across Python 3.11-3.13, explicitly covers Django 5.2 LTS and
Django 6.0, and runs an installed-wheel smoke test. `make wheel-smoke` runs that last one locally if you have
touched packaging, imports, or anything in `src/dewey/django/migrations/`.

## What we look for

**Tests that would fail without the change.** Concurrency and recovery bugs in particular:
if it involves two processes, a crash, or a duplicate delivery, the test should actually
create that situation. `tests/test_dispatcher.py` has examples using threads and real
Postgres.

**A migration when a model changes.** `make test` fails if models and migrations drift, so
run `python -m django makemigrations dewey` and commit the result.

**Sync, async and Django parity.** The three executors share their failure decision in
`dewey/core/execution.py` precisely so they cannot diverge. If you add behaviour to one,
either put it there or add a parity test in `tests/test_executor_policy_parity.py`.

**Docs that match.** Public behaviour is documented in `docs/`. A change to what Dewey
guarantees is a change to `docs/concepts.md`.

## Things likely to be declined

- **A scheduler.** Cron, Celery beat, Huey periodic and Kubernetes CronJobs already exist;
  the documented pattern is to have one of them call `create_task`.
- **Broker-side retries.** Dewey owns the attempt budget. Two retry engines over one task
  is how work runs twice.
- **`.delay()` on the decorator.** It re-couples producers to worker imports, which is the
  thing Dewey is breaking.
- **Auto-loading domain objects** into handlers. Handlers look up their own rows; that
  keeps Dewey out of your ORM.
- **New policy fields without a use case.** The policy surface is deliberately Tier 1
  only. Bring the scenario, not just the field.

## Reporting a bug

Include the Dewey version, Python version, ORM and driver (psycopg2 or psycopg3), and
Postgres version. For anything about lost or duplicated work, the state of the row helps
enormously:

```sql
SELECT id, task_type, status, attempts, max_attempts, scheduled_for,
       dispatching_at, started_at, completed_at, error
FROM task_entries WHERE id = '...';
```

## Security

Please do not open a public issue for a vulnerability — see [SECURITY.md](SECURITY.md).

## License

Contributions are accepted under the MIT license that covers the project.
