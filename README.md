# jdbc-storage

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/c2dc1b9b00454d4594a3a59de75c41e4)](https://www.codacy.com/app/SpineEventEngine/jdbc-storage?utm_source=github.com&utm_medium=referral&utm_content=SpineEventEngine/jdbc-storage&utm_campaign=badger)

Support of storage in JDBC-compliant databases.

To support working with different JDBC drivers, the library uses [Querydsl](http://www.querydsl.com/) internally. So the list of supported drivers depends on `Querydsl` and can be found [here](http://www.querydsl.com/static/querydsl/4.1.3/reference/html_single/#d0e1067).

To use a particular JDBC implementation, you need to configure `JdbcStorageFactory` with the corresponding JDBC URL. The JDBC driver should be supplied to the classpath. The library will generate queries depending on the JDBC URL.
