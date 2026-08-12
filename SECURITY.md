# Security policy

## Supported versions

Dewey is pre-1.0. Security fixes land on the latest released minor version; there are no
long-term support branches yet.

## Reporting a vulnerability

Please report privately, not as a public issue:

- Open a [private security advisory](https://github.com/frankapps-labs/dewey/security/advisories/new), or
- email **hello@frankapps.com** with `dewey security` in the subject.

Useful detail: affected version, what an attacker can achieve, and a reproduction if you
have one. We will acknowledge within a few working days, and will credit you in the
advisory unless you would rather we did not.

## What runs automatically

These are the checks in the repository, not a claim that they are sufficient:

- **CodeQL** analyses Dewey's Python source on every push and pull request to `main`, and
  weekly. It runs with `build-mode: none`, so analysis never executes project code.
- **Dependency advisories** are audited with `pip-audit` against the locked dependency
  set. The locked runtime set resolved from all published extras is a blocking CI gate.
  Dev and tooling dependencies are audited advisory-only, since they reach
  contributors rather than users.
- **Dependabot** proposes GitHub Actions and Python dependency updates weekly.
- **OpenSSF Scorecard** runs on `main` and publishes its result publicly. It uses no
  stored token: results are published with a short-lived Sigstore identity, which also
  means the Branch-Protection check reports as inconclusive rather than passing.

Releases publish to PyPI with Trusted Publishing (OIDC); there is no long-lived PyPI
token in this repository.

## Scope

Dewey is a library that stores and dispatches task rows. What is in scope:

- SQL injection or query construction flaws in Dewey's own queries
- a way to make Dewey execute a callable that was never registered as a task
- privilege or isolation failures in the claim path — one dispatcher taking work another
  has locked, or a claim escaping its transaction
- leaking task arguments or metadata into logs in a way the documented behaviour does not
  describe

Out of scope, because they are properties of your application rather than of Dewey:

- **What handlers do.** Dewey runs the callable you registered, with the arguments you
  stored. A handler that shells out or deserialises untrusted input is your code.
- **Who may call `create_task`.** Dewey has no authorisation model; it assumes the caller is
  already trusted. Do not expose `create_task` — or a `task_type` chosen by a client —
  directly to untrusted input.
- **Broker and database access control.** Anyone who can write to `task_entries` or your
  broker can cause work to run. Secure them as the sensitive infrastructure they are.
- **Secrets in task arguments.** Arguments are stored in plain JSON, readable by anyone with
  database access, and appear in `psql` output. Pass an ID and let the handler fetch the
  secret.
