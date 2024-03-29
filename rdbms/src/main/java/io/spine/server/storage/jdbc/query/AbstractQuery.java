/*
 * Copyright 2023, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Redistribution and use in source and/or binary forms, with or without
 * modification, must retain the above copyright notice and the following
 * disclaimer.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.spine.server.storage.jdbc.query;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import com.querydsl.core.types.dsl.PathBuilder;
import com.querydsl.sql.AbstractSQLQueryFactory;
import com.querydsl.sql.Configuration;
import com.querydsl.sql.RelationalPath;
import com.querydsl.sql.RelationalPathBase;
import com.querydsl.sql.SQLBaseListener;
import com.querydsl.sql.SQLCloseListener;
import com.querydsl.sql.SQLListener;
import com.querydsl.sql.SQLListenerContext;
import com.querydsl.sql.SQLQueryFactory;
import com.querydsl.sql.mysql.MySQLQueryFactory;
import io.spine.query.ColumnName;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.record.JdbcTableSpec;
import io.spine.server.storage.jdbc.record.column.IdColumn;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;

/**
 * The implementation base for the queries to an SQL-compliant database.
 *
 * @param <I>
 *         the type of the record identifiers
 * @param <R>
 *         the type of the queried records
 */
@SuppressWarnings("AbstractClassWithoutAbstractMethods" /* To prevent direct instantiation.*/)
public abstract class AbstractQuery<I, R extends Message> implements StorageQuery<I, R> {

    private final DataSourceWrapper dataSource;
    private final RelationalPathBase<Object> tablePath;
    private final PathBuilder<Object> pathBuilder;
    private final JdbcTableSpec<I, R> tableSpec;
    private @MonotonicNonNull AbstractSQLQueryFactory<?> defaultFactory;
    private @MonotonicNonNull MySQLQueryFactory mySqlFactory;

    protected AbstractQuery(Builder<I, R, ? extends Builder<I, R, ?, ?>,
                                    ? extends StorageQuery<I, R>> builder) {
        this.tableSpec = builder.tableSpec;
        var tableName = builder.tableSpec.tableName();
        this.dataSource = builder.dataSource;
        this.tablePath = new RelationalPathBase<>(Object.class, tableName, tableName, tableName);
        this.pathBuilder = new PathBuilder<>(Object.class, tableName);
    }

    /**
     * Obtains the {@linkplain RelationalPath path} of the target table for the query.
     *
     * @return the table path
     */
    @VisibleForTesting
    public RelationalPath<Object> table() {
        return tablePath;
    }

    /**
     * Obtains a query factory suitable for the majority of DB engines.
     *
     * @return the query factory
     *
     * @see #mySqlFactory() for MySQL-specific factory
     */
    @VisibleForTesting
    public synchronized AbstractSQLQueryFactory<?> factory() {
        if(defaultFactory == null) {
            defaultFactory = defaultFactory(dataSource);
        }
        return defaultFactory;
    }

    /**
     * Obtains a MySQL-specific query factory.
     *
     * @return the query factory
     */
    @VisibleForTesting
    public synchronized MySQLQueryFactory mySqlFactory() {
        if(mySqlFactory == null) {
            mySqlFactory = mySqlFactory(dataSource);
        }
        return mySqlFactory;
    }

    @Override
    public JdbcTableSpec<I, R> tableSpec() {
        return tableSpec;
    }

    /**
     * Returns a path builder for a column passed as an RDBMS-level column.
     */
    protected PathBuilder<Object> pathOf(TableColumn column) {
        return pathOf(column.name());
    }

    /**
     * Returns a path builder for the column storing identifiers.
     */
    protected PathBuilder<Object> pathOf(IdColumn<?> idColumn) {
        return pathOf(idColumn.columnName());
    }

    /**
     * Returns the path builder for the column by the given name.
     */
    protected PathBuilder<Object> pathOf(ColumnName column) {
        return pathOf(column.value());
    }

    /**
     * Returns the path builder for the column by the given name.
     *
     * <p>Uses a wildcard column type.
     */
    protected PathBuilder<Object> pathOf(String columnName) {
        return pathBuilder.get(columnName);
    }

    @SuppressWarnings("WeakerAccess" /* Available to SPI users. */)
    protected PathBuilder<Object> idPath() {
        return pathOf(idColumn());
    }

    /**
     * Creates a configured query factory suitable for most RDBMS engines.
     *
     * <p>The returned factory does not perform any query optimizations specific to the database
     * engine used. See {@link #mySqlFactory(DataSourceWrapper) mySqlFactory(dataSource)} for
     * the factory which optimizes the execution for MySQL databases.
     *
     * <p>All queries produced by the factory are
     * {@linkplain Connection#setAutoCommit(boolean) transactional}.
     *
     * <p>Both commits and rollbacks of transactions are done automatically
     * by the {@linkplain TransactionHandler transaction handler}.
     *
     * <p>All {@linkplain Connection connections} will be closed automatically
     * using {@link SQLCloseListener}.
     *
     * <p>To support iteration over {@link java.sql.ResultSet ResultSet} after a transaction commit,
     * {@link java.sql.ResultSet#HOLD_CURSORS_OVER_COMMIT HOLD_CURSORS_OVER_COMMIT} option is used
     * for the underlying {@linkplain Connection#setHoldability(int) connection}.
     *
     * @param dataSource
     *         the data source to produce connections
     * @return a new query factory
     */
    private static AbstractSQLQueryFactory<?> defaultFactory(DataSourceWrapper dataSource) {
        var connectionSupplier = new ConnectionSupplier(dataSource);
        var configuration = configuration(dataSource);
        return new SQLQueryFactory(configuration, connectionSupplier);
    }

    /**
     * Creates a new MySQL-specific query factory.
     *
     * <p>Transactional settings of the returned factory are the same as for
     * the {@linkplain #defaultFactory(DataSourceWrapper) default query factory}.
     *
     * <p>The created factory optimizes the inserts and updates into DB tables
     * by leveraging {@code INSERT ... ON DUPLICATE KEY UPDATE ...} queries
     * instead of {@code SELECT ...} with the consecutive {@code INSERT}/{@code UPDATE}.
     *
     * @param dataSource
     *         the data source to produce connections
     * @return a new query factory
     */
    @SuppressWarnings("WeakerAccess" /* Exposed to SPI users. */)
    protected static MySQLQueryFactory mySqlFactory(DataSourceWrapper dataSource) {
        checkNotNull(dataSource);
        var connectionSupplier = new ConnectionSupplier(dataSource);
        return new MySQLQueryFactory(configuration(dataSource), connectionSupplier);
    }

    @NonNull
    private static Configuration configuration(DataSourceWrapper dataSource) {
        var templates = dataSource.templates();
        var configuration = new Configuration(templates);
        configuration.addListener(TransactionHandler.INSTANCE);
        configuration.addListener(SQLCloseListener.DEFAULT);
        return configuration;
    }

    /**
     * Obtains a connection to the underlying data source.
     */
    private static final class ConnectionSupplier implements Supplier<Connection> {

        private final DataSourceWrapper dataSource;

        private ConnectionSupplier(DataSourceWrapper source) {
            dataSource = source;
        }

        @Override
        public Connection get() {
            var connection = dataSource
                    .getConnection(false)
                    .get();
            try {
                connection.setHoldability(HOLD_CURSORS_OVER_COMMIT);
                return connection;
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }
    }

    /**
     * An abstract builder for {@linkplain StorageQuery queries}.
     *
     * @param <I>
     *         type of identifiers of the queried records
     * @param <R>
     *         type of the queried records
     * @param <B>
     *         type of the builder, for return type covariance
     * @param <Q>
     *         type of the query built by the builder
     */
    public abstract static class Builder<I,
                                         R extends Message,
                                         B extends Builder<I, R, B, Q>,
                                         Q extends AbstractQuery<I, R>> {

        private DataSourceWrapper dataSource;
        private JdbcTableSpec<I, R> tableSpec;

        /**
         * Creates a new instance of the {@link StorageQuery} with respect to the preconditions.
         *
         * @return a new non-null instance of the query
         * @see #checkPreconditions()
         */
        public final Q build() {
            checkPreconditions();
            var result = doBuild();
            checkNotNull(result, "The query must not be null.");
            return result;
        }

        /**
         * Obtains typed reference to {@code this}.
         *
         * <p>This method provides return type covariance in builder setters.
         */
        protected abstract B getThis();

        /**
         * Checks the preconditions of the query construction.
         *
         * <p>Default implementation checks that the {@linkplain #dataSource data source}
         * and {@linkplain #tableSpec table spec} are both not {@code null}.
         *
         * <p>Override this method to modify these preconditions.
         *
         * @throws IllegalStateException
         *         upon a precondition violation
         */
        protected void checkPreconditions() throws IllegalStateException {
            checkState(dataSource != null, "Data source must not be `null`.");
            checkState(tableSpec != null, "Table spec must not be `null`.");
        }

        /**
         * Builds a new instance of the query.
         *
         * <p>The construction preconditions are checked before calling this method.
         *
         * @return a new non-{@code null} instance of the query.
         */
        protected abstract Q doBuild();

        /**
         * Sets the {@linkplain DataSourceWrapper data source} to be used for query execution.
         *
         * @param dataSource
         *         the data source to use
         */
        public B setDataSource(DataSourceWrapper dataSource) {
            this.dataSource = checkNotNull(dataSource);
            return getThis();
        }

        /**
         * Sets the specification of the table over which the operation is performed.
         *
         * @param spec
         *         the table specification
         */
        public B setTableSpec(JdbcTableSpec<I, R> spec) {
            this.tableSpec = checkNotNull(spec);
            return getThis();
        }

        /**
         * Returns the table specification, if previously set.
         *
         * <p>Otherwise, returns {@code null}.
         */
        public @Nullable JdbcTableSpec<I, R> tableSpec() {
            return tableSpec;
        }
    }

    /**
     * A handler for a transactional query.
     *
     * <p>{@linkplain Connection#commit() Commits} a transaction, that was successfully executed
     * or {@linkplain Connection#rollback() performs a rollback} for it otherwise.
     */
    @VisibleForTesting
    static class TransactionHandler extends SQLBaseListener {

        static final SQLListener INSTANCE = new TransactionHandler();

        @Override
        public void executed(SQLListenerContext context) {
            var connection = context.getConnection();
            if (connection != null) {
                try {
                    connection.commit();
                } catch (SQLException e) {
                    throw new DatabaseException(e);
                }
            }
        }

        @Override
        public void exception(SQLListenerContext context) {
            var connection = context.getConnection();
            if (connection != null) {
                try {
                    connection.rollback();
                } catch (SQLException e) {
                    throw new DatabaseException(e);
                }
            }
        }
    }
}
