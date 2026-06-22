# Configuration

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
