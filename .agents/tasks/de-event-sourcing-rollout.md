# Adopt the de-event-sourcing storage API of core-jvm (Phase H rollout)

Upstream plan: `core-jvm/.agents/tasks/de-event-sourcing-plan.md`, Phase H.
Branch: `de-event-sourcing`. Depends on core-jvm `2.0.0-SNAPSHOT.522`.

## Problem

core-jvm removed event-sourced aggregate loading. For storage vendors:

- `AggregateStorage`, `StorageFactory.createAggregateStorage`, and the published
  `AggregateStorageTest` fixture are **removed**. Aggregate latest state arrives
  via `createEntityRecordStorage`/`createRecordStorage` with `group == null`.
- `createRecordStorage` gained a `@Nullable StorageGroup group` parameter.
  Non-null groups arrive from the per-entity histories (`EntityEventStorage`,
  `EntityStateHistoryStorage`), both named after the entity state type.
  The vendor must allocate physical storage by the **(group, recordType)** pair.
- `createEntityStateHistoryStorage` may be invoked concurrently (delivery
  worker threads); the factory must tolerate that.

Without honoring the group, JDBC table identity (`RecordSpec.sourceType()` alone)
conflates: all event journals with each other **and** with the event log
(`sourceType == Event` everywhere), and an entity's state history with its
latest-state storage — with `TableSpecs` handing out a cached spec with
the wrong ID column type.

## Fix

Mechanical half (earlier commits on the branch): 3-arg `createRecordStorage`
signature, `RecordStorageDelegateTest` → `DelegatingRecordStorageTest` renames,
obsolete aggregate test suites deleted, version bumped (`.110`).

Substantive half (this change):

- `TableNames.of(recordType, group)`: grouped table name =
  sanitized group name + `_` + record type simple name
  (e.g. `spine_test_storage_StgProject_Event`). Naming settled with
  the product owner on 2026-08-04 (generic rule over semantic suffixes).
- `TableSpecs`: cache keyed by `SpecKey(sourceType, recordType, groupName)`
  in a `ConcurrentHashMap` (`computeIfAbsent`). Grouped specs ignore custom
  table names (registered per record type — cannot discriminate groups);
  custom column mappings still apply.
- `JdbcStorageFactory.createRecordStorage` threads the group into
  `JdbcRecordStorage`; new `tableSpecFor(spec, group)` overload.
- `JdbcRecordStorage`: group-accepting constructors; legacy ones delegate
  with `group = null` (`JdbcSessionStorage`, `tableCreationSql` unchanged).
- Deleted the dead `io.spine.server.storage.jdbc.aggregate` package
  (dangling `{@link AggregateStorage}` broke Dokka).
- `docs/tables.md`: "Grouped tables" section.

New Kotlin specs (H2): `GroupedTableAllocationSpec` (the vendor allocation
contract), `JdbcEntityEventStorageSpec`, `JdbcEntityStateHistoryStorageSpec`
(round-trips incl. `EntityStateKey` Message ID, upsert overwrite, `stateAt`,
`trim`, `truncate`), `ConcurrentHistoryCreationSpec`.

## Follow-ups (out of scope)

- Group-aware `setTableName` overload, if users ask for custom history names.
- Grouped-DDL export via `tableCreationSql` (currently latest-state only).

## Status

Implemented; `./gradlew build dokkaGenerate` green. Delete this file on merge
to master.
