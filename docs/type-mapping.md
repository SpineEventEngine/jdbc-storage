# SQL type mapping

The framework provides a `TypeMapping` to configure the SQL types, which fit the target storage.
The mapping defines correspondence of `Type` to a name for a particular database.
`Type` is an abstraction for a data type in a database.

The type mapping is selected automatically based on the database product name and version,
as reported by the JDBC driver's `DatabaseMetaData`.
If there is no predefined mapping for the database,
mapping for MySQL 9.7 will be used as the default.

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
