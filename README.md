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
    implementation("io.spine:spine-rdbms:1.8.2")
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

### SQL Type Mapping

The framework provides a `TypeMapping` to configure the SQL types, which fit the target storage.
The mapping defines correspondence of `Type` to a name for a particular database. 
`Type` is an abstraction for a data type in a database. 

The type mapping is selected automatically basing on the JDBC connection string.
If there is no predefined mapping for the database, mapping for MySQL 5.7 will be used as the default.

#### Default Values

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
