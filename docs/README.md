# `jdbc-storage` documentation

This directory contains the usage documentation for the `jdbc-storage` library.
For basic information and instructions on adding the library to a project,
see the [project README](../README.md).

## Contents

* [Configuration](configuration.md) — building a `JdbcStorageFactory` and
  plugging it into the `ServerEnvironment`.
* [SQL type mapping](type-mapping.md) — how data types map to SQL types,
  and how to customize the mapping.
* [RDBMS tables](tables.md) — table naming and structure, adding new columns,
  indexes, and customization. *(Available since 2.x.)*
* [Queries](queries.md) — default queries, RDBMS engine detection,
  and customization. *(Available since 2.x.)*

A generated report of the library's third-party dependencies is available
under [`dependencies`](dependencies/dependencies.md).
