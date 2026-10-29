# Run `CatchUpStorageTest` on the JDBC storage

Companion of SpineEventEngine/core-jvm#1699, which makes the catch-up jobs the entity records of
`CatchUp` process managers. `CatchUpStorage` becomes an `EntityRecordStorage<CatchUpId, CatchUp>`
with the columns `status`, `when_last_read` and `projection_type` declared via `(column)`.
core-jvm adds the abstract suite `io.spine.server.delivery.CatchUpStorageTest` to the
`server` test fixtures.

Merge only after #1699 merges and core-jvm `2.0.0-SNAPSHOT.610` is published.

## Steps

1. Move to core-jvm `.600` (newest published), aligning every `local/` pin with core-jvm's
   `buildSrc`. Then to `.610` from Maven Local.
2. `JdbcCatchUpStorageTest extends CatchUpStorageTest`, on the H2 factory the other
   delivery tests use, next to `JdbcInboxStorageTest`.
3. `MysqlCatchUpStorageTest`, on the shared MySQL container of `MysqlTests`. Like the other
   MySQL tests, it runs wherever Docker is present (`@EnableConditionally`), CI included.
   The "local only" rule of #1699's review applies to `core-jvm`, not to this repository.
4. Keep `JdbcCatchUpSmokeTest` green.
5. On MySQL: create the old job table (`MessageStorage<CatchUpId, CatchUp>`, `.600`), then
   write a job through the new `CatchUpStorage` (`.610`). Record the table names and
   whether the write fails.

## Status

- [x] 1. `.600`: no compile breaks; `:rdbms:test` 344 tests, 0 failures. `.610` (Maven Local,
  core-jvm `12adf4eb446`): no compile breaks either. The `local/` pins of #1699 equal master's.
- [x] 2, 3. Both classes implement the abstract `newStorage()`. 8/8 cases pass on H2 and on
  MySQL 8.0, also with `CI=true`.
- [x] 4. `JdbcCatchUpSmokeTest` green. A one-off run with `@Disabled` deactivated passed all 5.
- [x] 5. Old and new storages share `spine_server_catchup_CatchUp`. With the old table kept, with
  or without its rows, reads fail on `archived` and writes on `deleted`. After `DROP TABLE`,
  everything works. The new table adds `deleted`, `archived` and `version`. Report sent to
  the core-jvm session (shared notes, `jdbc-catch-up-table-upgrade.md`).
- [x] PR #187, opened after #1699 merged and `.610` was published.
- [x] `docs/catch-up-migration.md` tells users to drop the old job table on the upgrade.
