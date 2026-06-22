# SQL type mapping

The framework provides a `TypeMapping` to configure the SQL types, which fit the target storage.
The mapping defines correspondence of `Type` to a name for a particular database.
`Type` is an abstraction for a data type in a database.

The type mapping is selected automatically based on the database product name and version,
as reported by the JDBC driver's `DatabaseMetaData`.
If there is no predefined mapping for the database,
mapping for MySQL 9.7 will be used as the default.

## Default values

|    Type    |  MySQL 9.7   | PostgreSQL 10.1  |
|:----------:|:------------:|:----------------:|
| BYTE_ARRAY |     BLOB     |      BYTEA       |
|    INT     |     INT      |       INT        |
|    LONG    |    BIGINT    |      BIGINT      |
|   FLOAT    |    FLOAT     |       REAL       |
|   DOUBLE   |    DOUBLE    | DOUBLE PRECISION |
| STRING_255 | VARCHAR(255) |   VARCHAR(255)   |
| STRING_512 | VARCHAR(512) |   VARCHAR(512)   |
|   STRING   |     TEXT     |       TEXT       |
|  BOOLEAN   |   BOOLEAN    |     BOOLEAN      |

## Custom mapping

If the automatically selected mapping doesn't match your requirements, a custom mapping can be
specified during creation of `JdbcStorageFactory`.
The library exposes `TypeMappingBuilder.mappingBuilder()` shortcut, returning a builder
already containing the mappings for all data types, as per MySQL 9.7 mapping scheme.
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
