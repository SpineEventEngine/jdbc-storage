# Queries

*Available since 2.x.*

## Defaults

All operations against RDBMS tables which this library holds,
are created through `OperationFactory`. List of operations is available
via `io.spine.server.storage.jdbc.operation` package.

Each operation creates a corresponding query. Their default implementations are available
in `io.spine.server.storage.jdbc.query` package. Most of the queries use a vanilla SQL syntax
compatible with the majority of modern RDBMS engines. However, in its generic form,
`WriteOne` operation executes two queries: one to understand whether the record already exists,
and the second one to either `INSERT` or `UPDATE` the record by its ID.

Special support is provided for queries targeting MySQL. In particular, when MySQL engine
is detected from the provided data source, `WriteOne` is substituted by `MySqlWriteOne` operation,
which in turn utilizes an `INSERT ... ON DUPLICATE KEY UPDATE` syntax specific to this engine.
It allows to significantly enhance the performance for most typical scenarios, such as updating
an Entity state.

## RDBMS engine detection

By default, RDBMS engine is detected from the predefined list of engines.
See `io.spine.server.storage.jdbc.engine.PredefinedEngine` for more detail.

It is also possible to customize the engine, see more on that below.

## Customization

### Operations

As `OperationFactory` is a port, it is possible to customize its default behaviour
by providing a custom implementation. In scope of such a custom descendant, it is also
possible to use a custom operation (by choosing an existing operation
from `io.spine.server.storage.jdbc.operation` as a supertype) and a custom query
(by extending a query implementation from `io.spine.server.storage.jdbc.query`):

```java

// Custom operation factory, overriding `WriteOne` operation with a custom one.
public static final class CustomOpFactory extends OperationFactory {

    public CustomOpFactory(DataSourceWrapper wrapper, TypeMapping mapping) {
        super(wrapper, mapping);
    }

    @Override
    public <I, R extends Message> WriteOne<I, R> writeOne(RecordTable<I, R> table) {
        return new CustomWriteOne<>(table, dataSource());
    }
}

//...

// Custom `WriteOne` implementation.
public final class CustomWriteOne<I, R extends Message> extends WriteOne<I, R> { ... }

var factory = JdbcStorageFactory
        .newBuilder()

        // ...

        // Substitutes the default operation factory:
        .useOperationFactory(CustomOpFactory::new)

        // ...

        .build();
```

See `OperationFactoryTest` for a sample usage.

### Engine detection

End-users are also able to hard-code the engine by extending the `OperationFactory`
via its `protected OperationFactory(DataSourceWrapper, TypeMapping, DetectedEngine)` constructor.
Then, any overridden operations may get access to this value.
