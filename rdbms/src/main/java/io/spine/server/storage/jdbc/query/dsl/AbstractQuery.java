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

import com.querydsl.core.types.Order;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.dsl.PathBuilder;
import com.querydsl.sql.AbstractSQLQueryFactory;
import com.querydsl.sql.MySQLTemplates;
import com.querydsl.sql.RelationalPath;
import com.querydsl.sql.RelationalPathBase;
import com.querydsl.sql.SQLQueryFactory;
import com.querydsl.sql.SQLTemplates;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.query.StorageQuery;

import javax.inject.Provider;

import java.sql.Connection;

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

    private final AbstractSQLQueryFactory<?> queryFactory;
    private final RelationalPathBase<Object> tablePath;
    private final PathBuilder<Object> pathBuilder;

    AbstractQuery(Builder<? extends Builder, ? extends StorageQuery> builder) {
        final String tableName = builder.tableName;
        this.queryFactory = createFactory(builder.dataSource);
        this.tablePath = new RelationalPathBase<>(Object.class, tableName, tableName, tableName);
        this.pathBuilder = new PathBuilder<>(Object.class, tableName);
    }

    RelationalPath<Object> table() {
        return tablePath;
    }

    AbstractSQLQueryFactory<?> factory() {
        return queryFactory;
    }

    PathBuilder<Object> pathOf(TableColumn column) {
        return pathBuilder.get(column.name());
    }

    OrderSpecifier<Comparable> orderBy(TableColumn column, Order order) {
        final PathBuilder<Comparable> columnPath = pathBuilder.get(column.name(), Comparable.class);
        return new OrderSpecifier<>(order, columnPath);
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
