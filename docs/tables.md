# RDBMS tables

*Available since 2.x.*

## Naming and structure

Each Entity registered within the application's Bounded Contexts has a corresponding RDBMS table.
Additionally, the framework has some system Entities and other types (such as `InboxMessage`)
that are also stored in their tables.

For each type of stored records, the framework automatically creates an RDBMS table,
if it does not exist.

The name of the table is composed according to the following scheme:

```
(Package of Proto message + message name) -> (replace prohibited characters with `_`) -> result
```

Every character other than Latin letters, digits, and the underscore is prohibited
in a table name and gets replaced — for a Proto package, these are the `.` symbols.

E.g. a table name for an Entity that has a state declared by `bar.acme.Project` would be
"bar_acme_Project".

Each table created has the following structure:

* `ID` — the identifier of the record (Entity, or a standalone message). Primary key.
  On MySQL, string identifiers use a binary collation so that they are matched case-sensitively;
  see [Case sensitivity on MySQL](type-mapping.md#case-sensitivity-on-mysql).
* `bytes` — stores the serialized Proto message (Entity state, or a standalone message value).
* Columns defined either
     * via `Entity`'s `(column)` option;
     * or according to the columns declaration for a standalone message,
       annotated with `@RecordColumns` (e.g. `io.spine.server.event.store.EventColumn`).

> [!WARNING]
> The framework does **not** verify the table structure for existing tables.

## Grouped tables

Several storages of a Bounded Context may hold records of the same type.
For example, the per-entity histories introduced with Spine 2.x:

* the event journal of an entity type (`EntityEventStorage`) stores `Event`s —
  just as the journals of all other entity types, and the event log
  of the Bounded Context;
* the state history of an entity type (`EntityStateHistoryStorage`) stores
  `EntityRecord`s — just as the latest-state storage of the same entity type.

To keep such storages apart, the framework passes a `StorageGroup` when creating them,
named after the state type of the served entity. This library allocates a separate table
per the combination of the record specification and the group.

The name of a grouped table is composed of the group name and the simple name
of the stored record type:

```
(group name + record type name) -> (replace prohibited characters with `_`, join with `_`) -> result
```

E.g. for an Entity with the state declared by `bar.acme.Project`, the tables are:

| Storage                  | Table                           |
|--------------------------|---------------------------------|
| Latest state (ungrouped) | `bar_acme_Project`              |
| Event journal            | `bar_acme_Project_Event`        |
| State history            | `bar_acme_Project_EntityRecord` |

Grouped tables have the same structure as the ungrouped ones: the `ID` and `bytes`
columns, plus the columns declared for the stored record type — for both histories,
these are `entity_id`, `created`, and `version`.

A grouped table can also be given a custom name; see [Customization](#customization).

### The event log of a Bounded Context

The event store of each Bounded Context is a grouped storage, too: the framework
passes a `StorageGroup` named after the context, taking its name verbatim. The event
log of each context therefore lands in its own table. E.g., for an application with
the contexts named `Billing` and `Shipping`, the tables are `Billing_Event` and
`Shipping_Event`. A System context configured to persist its events — see
`SystemSettings.persistEvents()` — gets its own table as well, e.g. `Billing_System_Event`.

Applications upgrading from the library versions that kept a single
`spine_core_Event` table shared by all contexts should migrate the stored events;
see [Migrating the event log](event-log-migration.md).

> [!WARNING]
> Group names are the fully qualified names of Proto types or the names of
> Bounded Contexts, so the names of grouped tables run longer than the ungrouped ones.
> Mind the identifier length limits of the underlying DB engine when naming the Proto
> packages of entity states and the contexts. The limits differ per engine in both
> the value and the unit: e.g., MySQL rejects identifiers longer than 64 characters,
> while PostgreSQL silently truncates them to 63 bytes.

## Name clashes

Replacing the prohibited characters may map distinct names to one table name: e.g.,
the Bounded Contexts named `Sales.EU` and `Sales_EU` would both resolve to the
`Sales_EU_Event` table. The factory detects such a clash when the second of the
clashing storages is created, and fails with an `IllegalStateException` naming both
claimants. To resolve the clash, rename one of the contexts, or assign a distinct
table name via `setTableName(...)`; see [Customization](#customization).

The names are compared as the least discriminating of the supported engines would see
them, so two names are also treated as clashing when they differ only:

* past the 63rd byte — PostgreSQL truncates identifiers there silently; or
* in case — MySQL may run with a case-insensitive `lower_case_table_names` setting,
  which maps `Billing_Event` and `billing_Event` onto one table.

The second rule is deliberately conservative: PostgreSQL and H2 could keep such names
apart, but a Bounded Context layout that relies on letter case alone would not survive
a move to MySQL.

## Adding new `(column)`

In the scope of the development cycle, there may arise a need to modify the declaration of
Proto messages stored as records, by marking more fields with the `(column)` option.
In this case, it is important to understand that the framework will **not** be updating
the structure of existing tables in the underlying storage.

To handle such a scenario, developers should invoke a utility method on top of `JdbcStorageFactory`,
which prints out the SQL statement for the respective table _creation_:

```java
// A projection, which state is the `Project` Proto message.
public static final class MyProjection
        extends Projection<ProjectId, Project, Project.Builder> {
    // ...
}

var boundedContextSpec = // ...
var factory = JdbcStorageFactory.newBuilder()
                // ...
                .build();

// Receive the `CREATE TABLE` expression for the table
// storing the records for the given projection.
var createTableSql =
        factory.tableCreationSql(boundedContextSpec, MyProjection.class);
```

Then, by using the obtained `CREATE TABLE` expression, manually compose and execute
the SQL expression for altering the table, taking the specific features
of the underlying DB engine into account.

## Indexes

For both read-side and even write-side data structures, Spine end-users should
expect them to be queried via SQL. Most of the entity state records are always queried by their IDs,
but the records with `(column)`-annotated fields may also be queried by their values.

This library is generally agnostic to a particular RDBMS engine, and as of now, provides
no automatic detection of dialect- or engine-specific table optimizations.
Therefore, **no table indexes are automatically generated**.

Prior to production use, it is recommended to launch the Spine-based application
in a load-testing mode on top of the RDBMS of choice, analyze the usage scenarios,
and manually create indexes that suit the scenarios best.

## Customization

The library provides an API to customize the RDBMS tables used by storage instances.
It is available as a part of the `JdbcStorageFactory.Builder` API.

It is possible to configure several aspects:

* name of RDBMS table, per type of stored records:

```java

// A projection, using `TaskView` Proto message as a state type.
public final class TaskProjection
    extends Projection<TaskId, TaskView, TaskView.Builder> { ... }

var factory = JdbcStorageFactory.newBuilder()
        // ...

        // Uses the state type of an Entity to set the name for its table:
        .setTableName(TaskView.class, "my_favourite_tasks")

        // ...

        // It also works for "system" tables, keyed by the type of the stored record:
        .setTableName(InboxMessage.class, "custom_inbox_messages")
        .build();
```

> [!WARNING]
> The single-type `setTableName(...)` applies only to the storages outside any
> `StorageGroup`: a name set for an entity state type names the latest-state table alone;
> honoring it for the state history of the same entity would collide the two tables.
> In particular, a name set for `Event.class` does not apply to
> [the event log of a context](#the-event-log-of-a-bounded-context), which is grouped.

To name the [grouped tables](#grouped-tables) of an entity — its per-entity histories —
address them by the entity state type paired with the type of the stored records:

```java
var factory = JdbcStorageFactory.newBuilder()
        // ...

        // The event journal of the `Project` entities:
        .setTableName(Project.class, Event.class, "project_journal")

        // The state history of the `Project` entities:
        .setTableName(Project.class, EntityRecord.class, "project_state_history")
        .build();
```

The event log of a Bounded Context is addressed by the context name paired with
the type of the stored records:

```java
var factory = JdbcStorageFactory.newBuilder()
        // ...

        // The event log of the `Billing` Bounded Context:
        .setTableName(BoundedContextNames.newName("Billing"), Event.class, "billing_events")
        .build();
```

To address the table of a System context, spell its name directly:
`BoundedContextNames.newName("Billing_System")`.

* column type mapping, per type of stored records:

```java
// A projection, which state is the `Project` Proto message,
// stored as a record in the corresponding table.
public static final class MyProjection
        extends Projection<ProjectId, Project, Project.Builder> { ... }

// ...

// Sample mapping for `Project`-typed records
// stored in the corresponding RDBMS table.
public static class ProjectRecordMapping extends JdbcColumnMapping {

    // Convert `Timestamp`-typed column values into `Long`s by taking only seconds,
    // and dropping nanos.
    @Override
    protected ImmutableMap<Class<?>, JdbcColumnTypeMapping<?, ?>> customRules() {
        var timestampMapping =
                new JdbcColumnTypeMapping<Timestamp, Long>(
                        (value) -> (long) value.getSeconds(),
                        LONG);
        return ImmutableMap.of(
                Timestamp.class, timestampMapping
        );
    }
}

//...

var projectRecordMapping = new ProjectRecordMapping();
var factory = JdbcStorageFactory
        .newBuilder()
        .setCustomMapping(Project.class, projectRecordMapping)
        // ...
        .build();
```
