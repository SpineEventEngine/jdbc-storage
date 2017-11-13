# jdbc-storage

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/c2dc1b9b00454d4594a3a59de75c41e4)](https://www.codacy.com/app/SpineEventEngine/jdbc-storage?utm_source=github.com&utm_medium=referral&utm_content=SpineEventEngine/jdbc-storage&utm_campaign=badger)

Support of storage in JDBC-compliant databases.

To support working with different JDBC drivers, the library uses [Querydsl](http://www.querydsl.com/)
internally. So the list of supported drivers depends on `Querydsl` and can be found
[here](http://www.querydsl.com/static/querydsl/4.1.3/reference/html_single/#d0e1067).

To use a particular JDBC implementation, you need to configure `JdbcStorageFactory` with
the corresponding JDBC connection string.
 
The JDBC driver, corresponding to the target database, must be present in the project classpath.
This is a responsibility of a developer.

Here is an example of specifying the connection string:

```
final HikariConfig config = new HikariConfig();
config.setJdbcUrl("jdbc:mysql://localhost:3306/DbName");
        
final DataSource dataSource = new HikariDataSource(config);
JdbcStorageFactory.newBuilder()
                  .setDataSource(dataSource)
                  .build();
```
