# Per-context event tables: the `jdbc-storage` part

## Status

Plan approved; decision (d) taken: `JdbcDefaultEventStoreTest` is implemented in this
branch. § 0 done — `CoreJvm` bumped to `2.0.0-SNAPSHOT.540` (commit `bab7fd90`).

§§ 1–3 implemented: sanitization in `TableNames`, collision detection in `TableSpecs`,
the `setTableName(BoundedContextName, ..)` overloads, and the five test suites
(`TableNamesSpec`, `TableNameCollisionSpec`, `EventLogIsolationSpec`,
`ContextTableNameSpec`, `JdbcDefaultEventStoreTest`).

Unplanned but required: the root `build.gradle.kts` is adapted to the updated
`config`/buildSrc API (`JacksonV2` forcing groups, `Caffeine.lib`,
`coreJvmCompiler.gradlePlugin`, dropped monolithic `ToolBase.lib`), mirroring
core-jvm's script on the same `config` version.

Remaining: § 4 migration/release notes placement per repo convention.

## Background

Core-jvm PR [#1673](https://github.com/SpineEventEngine/core-jvm/pull/1673) made
`DefaultEventStore` create its record storage under a `StorageGroup` named after the
Bounded Context (`StorageGroup.of(BoundedContextName)`). The overall rationale and the
cross-repo scenario live in core-jvm's `.agents/tasks/event-store-context-prefix.md`.

This repo already honors groups: `TableSpecs` keys tables by
`(sourceType, recordType, group)` and names grouped tables via
`TableNames.of(recordType, group)`. Once the core-jvm dependency is bumped past #1673,
each context's event log lands in its own `<ContextName>_Event` table with **no code
changes here**. The work below hardens naming, closes the custom-name API gap, and
adds the missing coverage.

## 0. Prerequisite: core-jvm dependency bump

Bump `CoreJvm.version` in `buildSrc/src/main/kotlin/io/spine/dependency/local/CoreJvm.kt`
from `2.0.0-SNAPSHOT.522` to the first version published from core-jvm `master` that
contains #1673 (`.540` or later; fill in the exact number at implementation time).

Note: this bump alone flips the behavior — with the new core, every
`createEventStore(..)` arrives at `createRecordStorage(..)` with a context-derived
group. Everything below builds on that.

## 1. Legal and collision-safe table names

Two distinct concerns, solved by two distinct means.

**Legality — sanitize.** Context names are validated only as non-blank, so spaces,
dashes, and other characters illegal in SQL identifiers are possible. Broaden the
`TableNames` replacement from dots-only to every character outside `[A-Za-z0-9]` → `_`,
in both naming paths (`of(Class)` and `of(Class, StorageGroup)`). For names built of
letters, digits, and dots — every working deployment today — the output is
byte-for-byte identical to the current one: **nothing is renamed, ever**. Names with
other characters never produced a working table before, so nothing depends on them.

**Aliasing — detect, do not encode.** Sanitization is many-to-one: `Sales.EU`,
`Sales_EU`, and `Sales EU` all yield `Sales_EU` (flagged by the Codex review on
core-jvm #1673). A digest-suffix encoding was sketched in core-jvm's plan and
**dropped in review**: context names are few and human-chosen, an application cannot
have two identically-named contexts, and hex in table names serves nobody. Instead,
`TableSpecs` — which already caches every table spec it creates — detects the clash:
when a newly composed table name equals one already claimed by a different spec key,
throw `IllegalStateException` naming both claimants and the remediation (rename one
of the contexts, or assign an explicit table via `setTableName(..)`). A pathological
pair like `Sales.EU` + `Sales_EU` in one application then fails fast at storage
creation instead of silently mixing events. This satisfies the collision-free-mapping
contract in `StorageGroup`'s KDoc: the mapping that succeeds is injective.

Details:

- Detection compares effective names truncated to 63 bytes — PostgreSQL's identifier
  limit, applied silently by the server — so two long names differing only past that
  limit are caught too. (H2 and MySQL allow longer names; checking against the
  strictest bound costs nothing.)
- Custom names supplied via `setTableName(..)` participate in the same detection but
  are never sanitized (unchanged behavior: choosing a valid name is the caller's
  responsibility).
- The check is per-factory, which matches reality: one `ServerEnvironment` factory
  serves all contexts of an application. Separate factories over one database do not
  cross-check (pre-existing, out of scope).

This section supersedes the digest sketch in core-jvm's
`.agents/tasks/event-store-context-prefix.md` § 2.

## 2. Context-addressed custom table names in `TableSpecs.Builder`

Grouped custom names are currently registered only by entity state type
(`setTableName(Class<S> stateType, Class<R> recordType, String name)`), so the
context-grouped event table cannot be renamed. Add:

```java
@CanIgnoreReturnValue
public <R extends Message>
Builder setTableName(BoundedContextName context, Class<R> recordType, String name)
```

- Derives the group via `StorageGroup.of(BoundedContextName)` — the same single
  source of truth core uses — and stores into the existing `groupedNames` map.
- Javadoc: the context name is used verbatim (see `StorageGroup` docs); to address
  a System context's table, spell the name directly
  (`BoundedContextNames.newName("Billing_System")`), since `toSystem()` is internal.
- Release note: single-type `setTableName(Event.class, ..)` customizations no longer
  apply to the event table — it is a grouped table now.

## 3. Tests

- New Kotlin `TableNamesSpec` (the legacy Java `TableNamesTest` stays untouched):
  dot-only names keep today's exact mapping; illegal characters (space, dash) are
  sanitized to `_`.
- Collision detection in `TableSpecs`: two contexts `Sales.EU` and `Sales_EU` over
  one factory — the second storage creation throws, and the message names both
  claimants; a custom name colliding with a derived one is detected likewise; two
  names differing only past the 63rd byte are detected.
- Event-log isolation, modeled on the existing `GroupedTableAllocationSpec` and its
  `HistoryStorageTestEnv`/H2 harness (`GivenDataSource.whichIsStoredInMemory`):
  two `ContextSpec`s over one `JdbcStorageFactory`; `createEventStore(..)` for each;
  append distinct events; assert two `<Context>_Event` tables exist and each store
  reads back only its own events.
- Builder overload: a custom name registered for `(context, Event.class)` is honored
  by the event table; the single-type name for `Event.class` is not.
- Stretch (gap found while planning): this repo has no subclass of core's published
  `DefaultEventStoreTest` behavioral fixture. Adding `JdbcDefaultEventStoreTest`
  over H2 would exercise querying/tenancy against real tables. Optional; separate
  commit if taken.

## 4. Migration and release notes

- Existing deployments hold all contexts' events in `spine_core_Event`. Splitting by
  context uses the `type` column (qualified proto type name):

  ```sql
  INSERT INTO Billing_Event
      SELECT * FROM spine_core_Event
      WHERE type IN ('example.billing.InvoiceIssued', ...);
  ```

- No tables are renamed by this work — the sanitize-and-detect scheme keeps every
  existing derived name intact.
- `setTableName(Event.class, ..)` behavior change (see § 2).
- New failure mode to document: context (or type) names aliasing after sanitization
  now fail fast at storage creation with a remediation message.

## 5. Process

- Branch: `event-store-context-prefix` (mirrors core-jvm).
- Version bump per policy (once per branch).
- Commits roughly per section: dependency bump; encoding; Builder overload; tests
  ride with their sections; migration notes in `README`/release notes as the repo
  convention dictates.
- `pre-pr` gate before opening the PR against `master`.
