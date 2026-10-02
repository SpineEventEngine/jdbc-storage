---
slug: remove-test-deprecation-warnings
branch: claude/xenodochial-elbakyan-669620
owner: claude
status: in-review
started: 2026-10-02
---

## Goal

`:rdbms:compileTestJava` reports no `[deprecation]` warnings for
`ServerEnvironment.when(..)` or the Testcontainers 1.x-style
`org.testcontainers.containers.{MySQLContainer, PostgreSQLContainer}`, with
test behavior unchanged and the full build green (MySQL/Postgres suites
executed against Docker, not skipped).

## Context

- Warnings seen on 2026-10-02 at `origin/master` `c7479084`; they predate
  the FieldMask work on `drop-support-of-field-mask`.
- core-jvm `.551`: `when(type)` is `@Deprecated` and simply `return under(type);`
  — renamed only because `when` is a Kotlin keyword. Swap is behavior-identical.
- Testcontainers `2.0.5`: per-module replacements are
  `org.testcontainers.mysql.MySQLContainer` and
  `org.testcontainers.postgresql.PostgreSQLContainer`. Both are **non-generic**
  (`extends JdbcDatabaseContainer<Self>`), so `<?>`/`<>` must go.
  - Postgres: identical (same command, same log-message wait strategy built
    via `Wait.forLogMessage(..)`).
  - MySQL: the new class no longer copies the bundled `mysql-default-conf/my.cnf`
    (legacy low-memory tuning: buffer sizes, `max_allowed_packet = 1M`,
    `skip-name-resolve`) into `/etc/mysql/conf.d`. No charset, collation,
    `sql_mode`, or `lower_case_table_names` settings are involved — verify
    empirically.
- Version: `origin/master` has `.121` (latest published); `.122` is claimed by
  the unmerged `drop-support-of-field-mask` branch → this branch uses `.123`.
  Not committed, per the user's instruction.

## Plan

- [x] Baseline: capture `:rdbms:compileTestJava` warnings before edits
      — exactly 14 `[deprecation]`: 6 × `when`, 2 × Postgres, 6 × MySQL
- [x] Replace `ServerEnvironment.when(..)` with `under(..)` in 6 Java test
      classes, plus the Kotlin ``ServerEnvironment.`when`(..)`` in
      `JdbcDefaultEventStoreTest.kt` (reported by `compileTestKotlin`)
- [x] Migrate `MysqlTests` to `org.testcontainers.mysql.MySQLContainer`
- [x] Migrate `PostgresIdCaseSensitivityTest` to
      `org.testcontainers.postgresql.PostgreSQLContainer`
- [x] Update copyright headers of the touched files (`update-copyright`)
- [x] Bump `version.gradle.kts` → `2.0.0-SNAPSHOT.123` (uncommitted)
- [x] Verify: `./gradlew build dokkaGenerate`; no `[deprecation]` warnings
      for these APIs; MySQL/Postgres suites ran in `rdbms/build/test-results/test`
      — BUILD SUCCESSFUL; 346 tests, 0 failures, 18 skipped (the `@Disabled`
      delivery smoke tests). MySQL: 3 classes / 9 tests, Postgres: 1 test, none
      skipped. Forced full recompile (`--rerun`): 0 deprecation warnings.
- [x] Empirically diff MySQL server variables: old default conf vs stock image
      — 631 variables; only sizing differs (buffer pool, caches,
      `max_allowed_packet` 1 MiB → 64 MiB, perf-schema history sizes).
      Charset, collation, `sql_mode`, `lower_case_table_names`, isolation equal.

## Log

- 2026-10-02 — drafted from a fully specified user prompt; executing.
- 2026-10-02 — shared `version-bumped.sh` at the pinned `.agents/shared`
  (`9dfbd47`) cannot parse `extra.set(...)` (exit 2); fixed upstream in
  `agents@97ded99`. Ran the upstream script read-only: exit 1 → bumped.
- 2026-10-02 — out of scope, flagged: `docs/configuration.md` still shows
  `ServerEnvironment.when(..)` in a user-facing snippet.
- 2026-10-02 — verified; awaiting the user's review. Nothing committed.
