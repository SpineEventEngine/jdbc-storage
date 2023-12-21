[![experimental](http://badges.github.io/stability-badges/dist/experimental.svg)](http://github.com/badges/stability-badges)

# jdbc-storage

Support of storage in JDBC-compliant databases.

At this point, this library is **experimental**, and its API is a subject for future changes.

For the production use, consider NoSQL storage implementations such as Google Cloud Datastore. 
See [gcloud-java](https://github.com/SpineEventEngine/gcloud-java/) for Google Cloud support.
  
### Artifacts

Gradle:

```kotlin
dependencies {
    implementation("io.spine:spine-rdbms:1.9.0")
}
```

This artifact should be used as a part of the Spine server application.
 
For the details on setting up the server environment please refer to 
[Spine Bootstrap Gradle plugin](https://github.com/SpineEventEngine/bootstrap/) and 
[Spine `core` modules](https://github.com/SpineEventEngine/core-java/) documentation. 

### Configuration

To support working with different JDBC drivers, the library uses [Querydsl](http://www.querydsl.com/)
internally. So the list of supported drivers depends on `Querydsl` and can be found
[here](http://www.querydsl.com/static/querydsl/4.1.3/reference/html_single/#d0e1067).

To use a particular JDBC implementation, you need to configure `JdbcStorageFactory` with
the corresponding JDBC connection string.
 
The JDBC driver, corresponding to the target database, must be present in the project classpath.
This is a responsibility of a developer.

Here is an example of specifying the connection string:

```java
var config = new HikariConfig();
config.setJdbcUrl("jdbc:mysql://localhost:3306/DbName");
        
var dataSource = new HikariDataSource(config);
var factory = JdbcStorageFactory.newBuilder()
                  .setDataSource(dataSource)
                  .build();
```

Once built, the instance of `JdbcStorageFactory` should be plugged
into the current `ServerEnvironment`:

```java
var testingFactory = ...; 
var defaultFactory = ...;  
        
// Plug them into the environment.
ServerEnvironment
        
        // To use in tests:
        .when(Tests.class).useStorageFactory((env) -> testingFactory)
        
        // And in all other cases:
        .when(DefaultMode.class).useStorageFactory((env) -> defaultFactory)
```

### SQL type mapping

The framework provides a `TypeMapping` to configure the SQL types, which fit the target storage.
The mapping defines correspondence of `Type` to a name for a particular database. 
`Type` is an abstraction for a data type in a database. 

The type mapping is selected automatically basing on the JDBC connection string.
If there is no predefined mapping for the database, 
mapping for MySQL 5.7 will be used as the default.

#### Default values

|    Type    |  MySQL 5.7   | PostgreSQL 10.1 |
|:----------:|:------------:|:---------------:|
| BYTE_ARRAY |     BLOB     |      BYTEA      |
|    INT     |     INT      |       INT       |
|    LONG    |    BIGINT    |     BIGINT      |
| STRING_255 | VARCHAR(255) |  VARCHAR(255)   |
| STRING_512 | VARCHAR(512) |  VARCHAR(512)   |
|   STRING   |     TEXT     |      TEXT       |
|  BOOLEAN   |   BOOLEAN    |     BOOLEAN     |

#### Custom Mapping

If the automatically selected mapping doesn't match your requirements, a custom mapping can be
specified during creation of `JdbcStorageFactory`.  
The library exposes `TypeMappingBuilder.mappingBuilder()` shortcut, returning a builder
already containing the mappings for all data types, as per MySQL 5.7 mapping scheme.
The designed usage scenario is to override the values for required keys:

```java
var mapping = TypeMappingBuilder.mappingBuilder()
                   // Setting custom values for `INT` and `LONG`:
                  .add(Type.INT, "INT4")
                  .add(Type.LONG, "INT8")
                  .build();
var factory = JdbcStorageFactory.newBuilder()
                  .setTypeMapping(mapping)
                  //...
                  .build()
```

## Features available since 2.x

### RDBMS tables

#### Naming and structure

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

#### Adding new `(column)`

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

#### Indexes

For both read-side and even write-side data structures, Spine end-users should
expect them to be queried via SQL. Most of the entity state records are always queried by their IDs,
but the records with `(column)`-annotated fields may also be queried by their values.

This library is generally agnostic to a particular RDBMS engine, and as of now, provides
no automatic detection of dialect- or engine-specific table optimizations. 
Therefore, **no table indexes are automatically generated**.

Prior to production use, it is recommended to launch the Spine-based application 
in a load-testing mode on top of the RDBMS of choice, analyze the usage scenarios,
and manually create indexes which suit the scenarios best.

#### Customization

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


### Queries

#### Defaults

All operations against RDBMS tables which this library holds,
are created through `OperationFactory`. List of operations is available 
via `io.spine.server.storage.jdbc.operation` package.

Each operation creates a corresponding query. Their default implementations are available
in `io.spine.server.storage.jdbc.query` package. Most of the queries use a vanilla SQL syntax
compatible with the majority of modern RDBMS engines. However, in its generic form,
`WriteOne` operation executes two queries: one to understand whether the record already exists,
and the second one to either `INSERT` or `UPDATE` the record by its ID.

Special support is provided for queries targeting MySQL. In particular, when MySQL engine
is detected from the provided data source, `WriteOne` is substituted by `MySqlWriteOne` operation,
which in turn utilizes an `INSERT ... ON DUPLICATE KEY UPDATE` syntax specific to this engine.
It allows to significantly enhance the performance for most typical scenarios, such as updating
an Entity state.

#### RDBMS engine detection

By default, RDBMS engine is detected from the predefined list of engines.
See `io.spine.server.storage.jdbc.engine.PredefinedEngine` for more detail.

It is also possible to customize the engine, see more on that below.

#### Customization

##### Operations

As `OperationFactory` is a port, it is possible to customize its default behaviour 
by providing a custom implementation. In scope of such a custom descendant, it is also
possible to use a custom operation (by choosing an existing operation 
from `io.spine.server.storage.jdbc.operation` as a supertype) and a custom query
(by extending a query implementation from `io.spine.server.storage.jdbc.query`):

```java

// Custom operation factory, overriding `WriteOne` operation with a custom one.
public static final class CustomOpFactory extends OperationFactory {

    public CustomOpFactory(DataSourceWrapper wrapper, TypeMapping mapping) {
        super(wrapper, mapping);
    }

    @Override
    public <I, R extends Message> WriteOne<I, R> writeOne(RecordTable<I, R> table) {
        return new CustomWriteOne<>(table, dataSource());
    }
}

//...

// Custom `WriteOne` implementation.
public final class CustomWriteOne<I, R extends Message> extends WriteOne<I, R> { ... }    

var factory = JdbcStorageFactory
        .newBuilder()

        // ...

        // Substitutes the default operation factory:
        .useOperationFactory(CustomOpFactory::new)

        // ...
        
        .build();
```

See `OperationFactoryTest` for a sample usage

##### Engine detection

End-users are also able to hard-code the engine by extending the `OperationFactory`
via its `protected OperationFactory(DataSourceWrapper, TypeMapping, DetectedEngine)` constructor.
Then, any overridden operations may get access to this value.