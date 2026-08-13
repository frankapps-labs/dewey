# Dependency admission

Dependency updates are executable supply-chain changes, not routine lockfile noise. A clean
vulnerability scan means no known advisory matched; it does not prove that a newly published
artifact or maintainer account is trustworthy.

## Default path

Dependabot proposes ordinary Python and GitHub Actions version updates only after a seven-day
cooldown. Dependabot security updates are not delayed by cooldown.

Keep each Dependabot pull request on its bot-owned branch. When updates overlap, merge them
one at a time and let the remaining pull requests refresh on current `main`; do not combine
unrelated bot updates into an opaque lockfile batch.

Every dependency pull request must pass:

```bash
make dependency-check
make lint typecheck format-check test
```

CI additionally runs the supported Python/Django/Huey matrix, installed-wheel smoke,
optional-extra imports, runtime vulnerability audit, CodeQL, and workflow checks.

Before approval, review the dependency inventory and answer:

1. Is this an expected package and version? Treat new direct or transitive packages as new
   code entering the build.
2. Did the source, maintainer set, project ownership, build backend, install behavior, or
   package footprint change unexpectedly?
3. Is the release unusually recent, yanked, disputed, or missing expected provenance?
4. Does a major update alter Dewey's public compatibility contract or CI matrix?
5. Are lockfile changes limited to the intended dependency graph?

Major updates, new packages, source changes, and maintainer/project transfers require
explicit human review. Auto-merge is not an admission decision.

## What the automated gate proves

`make dependency-check` runs a standard-library checker before dependency installation and
then the vulnerability audits. It requires:

- an up-to-date `uv.lock`;
- registry packages sourced only from canonical PyPI;
- every selected PyPI artifact to carry a full SHA-256 digest;
- no direct Git, URL, path, or mutable source requirements;
- every external GitHub Action to use a full immutable commit SHA;
- blocking runtime `pip-audit` checks for the oldest and newest supported interpreter
  contexts;
- a visible advisory audit for development tooling.

The lockfile freezes Dewey's CI and contributor environment. It does **not** freeze a
consumer's environment: Dewey's published wheel intentionally exposes bounded dependency
ranges, so downstream applications must maintain their own lock and admission policy.

Hashes establish artifact identity after selection; they do not establish that the selected
artifact is benign. Vulnerability databases primarily find known issues. Human review,
cooldown, least-privilege CI, compatibility tests, and package minimization remain necessary.

## Urgent hotfix bypass

A known urgent fix may bypass the seven-day wait. This is an exception to **age only**, not
to any integrity or verification gate.

1. Create a maintainer branch from current `main`; do not wait for or rewrite a Dependabot
   branch.
2. Put both machine-readable lines in the pull request body:

   ```text
   Hotfix rationale: waiting creates greater risk because …
   Upstream: https://…
   ```

3. Ask a maintainer to apply the `dependency-hotfix` label. The label, rationale, and
   upstream HTTPS reference must all be present; the required CI gate validates them.
4. Update only the intended package and regenerate the lock deterministically, for example:

   ```bash
   uv lock --upgrade-package PACKAGE
   ```

5. Run `make dependency-check` and the normal compatibility/wheel gates.
6. Require human approval and merge normally. Close or allow Dependabot to supersede any
   duplicate proposal.

Security updates raised by Dependabot already bypass Dependabot's proposal cooldown. If the
selected PyPI artifact is itself less than seven days old, use the same explicit
`dependency-hotfix` admission—the urgency is real, but artifact age does not establish trust.
Do not lower or remove the repository-wide cooldown for a one-off exception.

## GitHub Actions updates

Actions are executable CI dependencies. Keep full-SHA pins and verify that the new SHA
belongs to the expected upstream release. Major action updates remain separate pull requests
and require review of permissions, runtime changes, and release notes.
