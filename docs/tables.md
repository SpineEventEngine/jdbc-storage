# RDBMS tables

*Available since 2.x.*

## Naming and structure

Each Entity registered within application's Bounded Contexts has a corresponding RDBMS table.
Additionally, the framework has some system Entities and other types (such as `InboxMessage`)
which are also stored in their tables.

For each type of stored records, the framework automatically creates an RDMBS table,
if it does not exist.

The name of the table is composed according to the following scheme:

```
(Package of Proto message + message name) -> (replace `.` with `_`) -> result
```

E.g. a table name for an Entity, which has a state declared by `bar.acme.Project` would be
"bar_acme_Project".

Each table created has the following structure:

* `ID` — the identifier of the record (Entity, or a standalone message). Primary key.
* `bytes` — stores the serialized Proto message (Entity state, or a standalone message value).
* Columns defined either
     * via `Entity`'s `(column)` option;
     * or according to the columns declaration for a standalone message,
       annotated with `@RecordColumns` (e.g. `io.spine.server.event.store.EventColumn`).

:warning: The framework does **not** verify the table structure for existing tables.

## Adding new `(column)`

In scope of development cycle, there may arise a need to modify the declaration of
Proto messages stored as records, by marking more fields with `(column)` option.
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
var factory = JdbcStorageFactory
                .newBuilder()
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
and manually create indexes which suit the scenarios best.

## Customization

The library provides an API to customize the RDBMS tables used by storage instances.
It is available as a part of `JdbcStorageFactory.Builder` API.

It is possible to configure several aspects:

* name of RDBMS table, per type of stored records:

```java

// A projection, using `TaskView` Proto message as a state type.
public final class TaskProjection
    extends Projection<TaskId, TaskView, TaskView.Builder> { ... }

var factory = JdbcStorageFactory
        .newBuilder()

        // ...

        // Uses the record type to set the name for its table:
        .setTableName(TaskView.class, "my_favourite_tasks")

        // ...

        // It also works for "system" tables:
        .setTableName(InboxMessage.class, "custom_inbox_messages")
        .build();
```

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
