/*
 * Copyright 2017, TeamDev Ltd. All rights reserved.
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

package io.spine.server.storage.jdbc.query.dsl;

import com.querydsl.sql.AbstractSQLQueryFactory;
import com.querydsl.sql.MySQLTemplates;
import com.querydsl.sql.RelationalPath;
import com.querydsl.sql.RelationalPathBase;
import com.querydsl.sql.SQLQueryFactory;
import com.querydsl.sql.SQLTemplates;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.query.StorageQuery;

import javax.inject.Provider;

import java.sql.Connection;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * The implementation base for the queries to an SQL database.
 *
 * @author Andrey Lavrov
 * @author Dmytro Dashenkov
 */
abstract class AbstractQuery implements StorageQuery {

    private final AbstractSQLQueryFactory<?> factory;
    private final String tableName;

    AbstractQuery(Builder<? extends Builder, ? extends StorageQuery> builder) {
        this.factory = createFactory(builder.dataSource);
        this.tableName = builder.tableName;
    }

    String getTableName() {
        return tableName;
    }

    RelationalPath<Object> getTable() {
        try {
            final String schema = factory.getConnection()
                                         .getSchema();
            return new RelationalPathBase<>(Object.class, tableName, schema, tableName);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    AbstractSQLQueryFactory<?> factory() {
        return factory;
    }

    private static AbstractSQLQueryFactory<?> createFactory(final DataSourceWrapper dataSource) {
        final Provider<Connection> connectionProvider = new Provider<Connection>() {
            @Override
            public Connection get() {
                return dataSource.getConnection(false)
                                 .get();
            }
        };
        final SQLTemplates templates = new MySQLTemplates();
        return new SQLQueryFactory(templates, connectionProvider);
    }

    abstract static class Builder<B extends AbstractQuery.Builder<B, Q>, Q extends AbstractQuery> {

        private DataSourceWrapper dataSource;
        private String tableName;

        abstract Q build();

        abstract B getThis();

        B setDataSource(DataSourceWrapper dataSource) {
            this.dataSource = checkNotNull(dataSource);
            return getThis();
        }

        B setTableName(String tableName) {
            checkArgument(!isNullOrEmpty(tableName));
            this.tableName = tableName;
            return getThis();
        }
    }
}
