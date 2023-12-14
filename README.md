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
HikariConfig config = new HikariConfig();
config.setJdbcUrl("jdbc:mysql://localhost:3306/DbName");
        
DataSource dataSource = new HikariDataSource(config);
JdbcStorageFactory.newBuilder()
                  .setDataSource(dataSource)
                  .build();
```

### SQL type mapping

The framework provides a `TypeMapping` to configure the SQL types, which fit the target storage.
The mapping defines correspondence of `Type` to a name for a particular database. 
`Type` is an abstraction for a data type in a database. 

The type mapping is selected automatically basing on the JDBC connection string.
If there is no predefined mapping for the database, mapping for MySQL 5.7 will be used as the default.

#### Default values

| Type         | MySQL 5.7     | PostgreSQL 10.1 |
| :----------: |:-------------:| :--------------:|
| BYTE_ARRAY   | BLOB          | BYTEA           |
| INT          | INT           | INT             |
| LONG         | BIGINT        | BIGINT          |
| STRING_255   | VARCHAR(255)  | VARCHAR(255)    | 
| STRING       | TEXT          | TEXT            |
| BOOLEAN      | BOOLEAN       | BOOLEAN         |

#### Custom Mapping

If the automatically selected mapping doesn't match your requirements, a custom mapping can be
specified during creation of `JdbcStorageFactory`. There is the builder for this purpose - 
`TypeMappingBuilder.basicBuilder()`. This builder contains mappings for all types
(equal to MySQL 5.7 mapping). So only required types should be overridden:

```java
TypeMapping mapping = TypeMappingBuilder.basicBuilder()
                                        .add(Type.INT, "INT4")
                                        .add(Type.LONG, "INT8")
                                        .build();
JdbcStorageFactory.newBuilder()
                  .setTypeMapping(mapping)
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

// A projection, which state is stored as a record for `Project` Proto message.
public static final class MyProjection
        extends Projection<ProjectId, Project, Project.Builder> {
    // ...
}

var boundedContextSpec = // ...
var factory = JdbcStorageFactory
                .newBuilder()
                // ...
                .build();

// Receive the `CREATE TABLE` expression for this record.
var createTableSql = 
        factory.tableCreationSql(boundedContextSpec, ProjectId.class, Project.class);

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

- Customization


### Queries

- Default behaviour

- Special support for MySQL

- Customization