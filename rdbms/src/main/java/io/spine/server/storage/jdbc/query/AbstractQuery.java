/*
 * Copyright 2021, TeamDev. All rights reserved.
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
import com.querydsl.core.types.Order;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.dsl.ComparablePath;
import com.querydsl.core.types.dsl.PathBuilder;
import com.querydsl.core.types.dsl.SimpleExpression;
import com.querydsl.sql.AbstractSQLQueryFactory;
import com.querydsl.sql.Configuration;
import com.querydsl.sql.RelationalPath;
import com.querydsl.sql.RelationalPathBase;
import com.querydsl.sql.SQLBaseListener;
import com.querydsl.sql.SQLCloseListener;
import com.querydsl.sql.SQLListener;
import com.querydsl.sql.SQLListenerContext;
import com.querydsl.sql.SQLQueryFactory;
import com.querydsl.sql.SQLTemplates;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.TableColumn;

import javax.inject.Provider;
import java.sql.Connection;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;

/**
 * The implementation base for the queries to an SQL-compliant database.
 */
public abstract class AbstractQuery implements StorageQuery {

    /**
     * The default main table alias for usage in sub-queries.
     */
    private static final String DEFAULT_TABLE_ALIAS = "main_table";

    private final AbstractSQLQueryFactory<?> queryFactory;
    private final RelationalPathBase<Object> tablePath;
    private final PathBuilder<Object> pathBuilder;
    private final PathBuilder<Object> aliasedPathBuilder;

    protected AbstractQuery(Builder<? extends Builder, ? extends StorageQuery> builder) {
        String tableName = builder.tableName;
        this.queryFactory = createFactory(builder.dataSource);
        this.tablePath = new RelationalPathBase<>(Object.class, tableName, tableName, tableName);
        this.pathBuilder = new PathBuilder<>(Object.class, tableName);
        this.aliasedPathBuilder = new PathBuilder<>(Object.class, DEFAULT_TABLE_ALIAS);
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

    protected SimpleExpression<Object> tableAlias() {
        return tablePath.as(DEFAULT_TABLE_ALIAS);
    }

    /**
     * Obtains a {@linkplain AbstractSQLQueryFactory factory} to compose the query.
     *
     * @return the query factory
     */
    @VisibleForTesting
    public AbstractSQLQueryFactory<?> factory() {
        return queryFactory;
    }

    protected PathBuilder<Object> pathOf(TableColumn column) {
        return pathOf(column.name());
    }

    protected PathBuilder<Object> pathOf(IdColumn<?> idColumn) {
        return pathOf(idColumn.columnName());
    }

    protected PathBuilder<Object> pathOf(String columnName) {
        return pathBuilder.get(columnName);
    }

    protected <T> PathBuilder<T> pathOf(String columnName, Class<T> type) {
        return pathBuilder.get(columnName, type);
    }

    protected <T extends Comparable> ComparablePath<T>
    comparablePathOf(TableColumn column, Class<T> type) {
        return pathBuilder.getComparable(column.name(), type);
    }

    protected PathBuilder<Object> aliasedPathOf(TableColumn column) {
        return aliasedPathBuilder.get(column.name());
    }

    protected <T extends Comparable> ComparablePath<T>
    aliasedComparablePathOf(TableColumn column, Class<T> type) {
        return aliasedPathBuilder.getComparable(column.name(), type);
    }

    protected OrderSpecifier<Comparable> orderBy(TableColumn column, Order order) {
        PathBuilder<Comparable> columnPath = pathBuilder.get(column.name(), Comparable.class);
        return new OrderSpecifier<>(order, columnPath);
    }

    /**
     * Creates a configured query factory.
     *
     * <p>All queries produced by the factory will be
     * {@linkplain Connection#setAutoCommit(boolean) transactional}.
     *
     * <p>A commit or rollback for a transaction will be done automatically
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
     * @return the query factory
     */
    @VisibleForTesting
    static AbstractSQLQueryFactory<?> createFactory(final DataSourceWrapper dataSource) {
        Provider<Connection> connectionProvider = () -> {
            Connection connection = dataSource.getConnection(false)
                                              .get();
            try {
                connection.setHoldability(HOLD_CURSORS_OVER_COMMIT);
                return connection;
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        };
        SQLTemplates templates = dataSource.templates();
        Configuration configuration = new Configuration(templates);
        configuration.addListener(TransactionHandler.INSTANCE);
        configuration.addListener(SQLCloseListener.DEFAULT);
        return new SQLQueryFactory(configuration, connectionProvider);
    }

    /**
     * An abstract builder for {@linkplain StorageQuery queries}.
     */
    protected abstract static class Builder<B extends Builder<B, Q>, Q extends AbstractQuery> {

        private DataSourceWrapper dataSource;
        private String tableName;

        /**
         * Creates a new instance of the {@link StorageQuery} with respect to the preconditions.
         *
         * @return a new non-null instance of the query
         * @see #checkPreconditions()
         */
        public final Q build() {
            checkPreconditions();
            Q result = doBuild();
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
         * <p>Default implementation checks that the {@linkplain #dataSource data source} is not
         * {@code null} and {@linkplain #tableName table name} is not an empty string.
         *
         * <p>Override this method to modify these preconditions.
         *
         * @throws IllegalStateException
         *         upon a precondition violation
         */
        protected void checkPreconditions() throws IllegalStateException {
            checkState(dataSource != null, "Data source must not be null");
            checkState(!isNullOrEmpty(tableName), "Table name must be a non-empty string.");
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
         * Sets the table name to use as a target for the query.
         *
         * @param tableName
         *         the table name for the query
         */
        public B setTableName(String tableName) {
            checkArgument(!isNullOrEmpty(tableName));
            this.tableName = tableName;
            return getThis();
        }
    }

    /**
     * A handler for a transactional query.
     *
     * <p>{@linkplain Connection#commit() Commits} a transaction, that was successfully executed
     * or {@linkplain Connection#rollback() rollbacks} it otherwise.
     */
    @VisibleForTesting
    static class TransactionHandler extends SQLBaseListener {

        static final SQLListener INSTANCE = new TransactionHandler();

        @Override
        public void executed(SQLListenerContext context) {
            Connection connection = context.getConnection();
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
            Connection connection = context.getConnection();
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
