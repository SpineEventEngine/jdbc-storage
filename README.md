[![experimental](http://badges.github.io/stability-badges/dist/experimental.svg)](http://github.com/badges/stability-badges)

# jdbc-storage

A [Spine Event Engine](https://github.com/SpineEventEngine) storage port for
JDBC-compliant databases. It connects a Spine-based application to the RDBMS of choice.

At this point, this library is **experimental**, and its API is subject to change.

For production use, consider NoSQL storage implementations such as Google Cloud Datastore.
See [gcloud-java](https://github.com/SpineEventEngine/gcloud-java/) for Google Cloud support.

For the details on setting up the server environment, please refer to the
[Spine Bootstrap Gradle plugin](https://github.com/SpineEventEngine/bootstrap/) and
[Spine `core` modules](https://github.com/SpineEventEngine/core-java/) documentation.

## Adding to your build

### Stable version

The stable version requires Java 8+.

Use the following dependency in your Gradle build scripts:

```kotlin
dependencies {
    implementation("io.spine:spine-rdbms:1.9.0")
}
```

### 2.x

Version 2.x is still in development, but as of now, it fully supports all major features
brought by the Spine 2.x family.

It requires Java 17+.

Use the following dependency in your Gradle build scripts:

```kotlin
dependencies {
    implementation("io.spine:jdbc-rdbms:$version")
}
```

:warning: The exact snapshot version for version 2.x is listed in `version.gradle.kts`.

## Documentation

The usage documentation lives in the [`docs`](docs) directory:

* [Configuration](docs/configuration.md) — building a `JdbcStorageFactory` and
  plugging it into the `ServerEnvironment`.
* [SQL type mapping](docs/type-mapping.md) — how data types map to SQL types,
  and how to customize the mapping.
* [RDBMS tables](docs/tables.md) — table naming and structure, adding new columns,
  indexes, and customization.
* [Queries](docs/queries.md) — default queries, RDBMS engine detection,
  and customization.

## License

This library is released under the [Apache License, Version 2.0](LICENSE).
