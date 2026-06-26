# SQL type mapping

The framework provides a `TypeMapping` to configure the SQL types, which fit the target storage.
The mapping defines correspondence of `Type` to a name for a particular database.
`Type` is an abstraction for a data type in a database.

The type mapping is selected automatically based on the database product name and version,
as reported by the JDBC driver's `DatabaseMetaData`.
If there is no exact match, a recognized database (MySQL, PostgreSQL, or H2) reported at another
version still uses that product's mapping — its dialect-specific type names apply across versions.
Only an unrecognized database falls back to a neutral mapping with portable type names and no
dialect-specific clauses.

## Default values

|    Type    |          MySQL 9.7          | PostgreSQL 10.1  |
|:----------:|:---------------------------:|:----------------:|
| BYTE_ARRAY |            BLOB             |      BYTEA       |
|    INT     |             INT             |       INT        |
|    LONG    |           BIGINT            |      BIGINT      |
|   FLOAT    |            FLOAT            |       REAL       |
|   DOUBLE   |           DOUBLE            | DOUBLE PRECISION |
| STRING_255 | VARCHAR(255) <sup>(1)</sup> |   VARCHAR(255)   |
| STRING_512 | VARCHAR(512) <sup>(1)</sup> |   VARCHAR(512)   |
|   STRING   |     TEXT <sup>(1)</sup>     |       TEXT       |
|  BOOLEAN   |           BOOLEAN           |     BOOLEAN      |

<sup>(1)</sup> On MySQL the character-based types additionally carry a
`CHARACTER SET utf8mb4 COLLATE utf8mb4_bin` clause. See
[Case sensitivity on MySQL](#case-sensitivity-on-mysql) below.

## Case sensitivity on MySQL

MySQL compares non-binary string types (`VARCHAR`, `TEXT`)
[case- and accent-insensitively](https://dev.mysql.com/doc/refman/8.4/en/case-sensitivity.html)
by default. Entity identifiers and `String` columns are stored in such types, so two values that
differ only by case — for example, a username `"name"` and a distinct username `"Name"` — would
be treated as equal. Commands addressed to one entity would then be routed to the other, and the
two records would collide on a single row.

To prevent this, the predefined MySQL mapping appends an explicit binary collation
(`CHARACTER SET utf8mb4 COLLATE utf8mb4_bin`) to every character-based column type, restoring
exact, case-sensitive matching. The collation does not change how many bytes a value occupies, so
the `VARCHAR(512)` primary key stays within InnoDB's index-length limit.

> ⚠️ **Do not drop the binary collation from MySQL string columns.** Reverting them to a plain
> `VARCHAR`/`TEXT` (or to a `_ci` collation) reintroduces the collision described above. When
> supplying a [custom mapping](#custom-mapping) for MySQL, keep the collation on the
> `STRING_255`, `STRING_512`, and `STRING` types.

PostgreSQL and H2 compare string data case-sensitively by default and therefore need no such
collation.

### Migrating existing tables

The framework never changes the structure of a table that already exists, so a table created
before this change keeps its original, case-insensitive collation — only newly created tables
get the binary collation automatically. Re-collate the character columns of each existing table
by hand. The simplest way is to convert the whole table:

```sql
ALTER TABLE `<table>` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
```

This re-collates every `VARCHAR`/`TEXT` column — the `ID` and all string columns — to the binary
collation. For a table already stored as `utf8mb4` the character set is unchanged, so only the
collation metadata and the affected indexes are rebuilt. To re-collate a single column instead:

```sql
ALTER TABLE `<table>` MODIFY `ID` VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL;
```

> ⚠️ Re-collating prevents *future* collisions but cannot undo past ones. If `"name"` and
> `"Name"` were both written while the column was case-insensitive, the table already holds a
> single merged row and the overwritten record is gone — audit such tables before migrating.

## Custom mapping

If the automatically selected mapping doesn't match your requirements, a custom mapping can be
specified during creation of `JdbcStorageFactory`.
The library exposes `TypeMappingBuilder.mappingBuilder()` shortcut, returning a builder
already containing default names for all data types.
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

The names returned by `mappingBuilder()` are the plain defaults shared by all databases; the
predefined `MYSQL_9_7` mapping layers the
[binary collation](#case-sensitivity-on-mysql) on top of them. When you build a custom mapping
for MySQL, add the collation to the `STRING_255`, `STRING_512`, and `STRING` types yourself, e.g.
`.add(Type.STRING_512, "VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")`.
