/*
 * Copyright 2022, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.spine.server.ContextSpec;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.aggregate.AggregateStorage;
import io.spine.server.delivery.CatchUpStorage;
import io.spine.server.delivery.InboxStorage;
import io.spine.server.entity.Entity;
import io.spine.server.projection.Projection;
import io.spine.server.projection.ProjectionStorage;
import io.spine.server.storage.StorageFactory;
import io.spine.server.storage.jdbc.aggregate.JdbcAggregateStorage;
import io.spine.server.storage.jdbc.delivery.JdbcCatchUpStorage;
import io.spine.server.storage.jdbc.delivery.JdbcInboxStorage;
import io.spine.server.storage.jdbc.delivery.JdbcSessionStorage;
import io.spine.server.storage.jdbc.projection.JdbcProjectionStorage;
import io.spine.server.storage.jdbc.record.JdbcRecordStorage;
import io.spine.server.storage.jdbc.type.DefaultJdbcColumnMapping;
import io.spine.server.storage.jdbc.type.JdbcColumnMapping;

import javax.sql.DataSource;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Creates storages based on JDBC-compliant RDBMS.
 *
 * @see DataSourceConfig
 */
public class JdbcStorageFactory implements StorageFactory {

    private final DataSourceWrapper dataSource;
    private final JdbcColumnMapping<?> columnMapping;
    private final TypeMapping typeMapping;

    private JdbcStorageFactory(Builder builder) {
        this.dataSource = checkNotNull(builder.dataSource);
        this.columnMapping = builder.columnMapping;
        this.typeMapping = checkNotNull(builder.typeMapping);
    }

    @Override
    public <I> AggregateStorage<I> createAggregateStorage(
            ContextSpec context, Class<? extends Aggregate<I, ?, ?>> aggregateClass) {
        JdbcAggregateStorage<I> storage =
                JdbcAggregateStorage.<I>newBuilder()
                        .setAggregateClass(aggregateClass)
                        .setMultitenant(context.isMultitenant())
                        .setDataSource(dataSource)
                        .setTypeMapping(typeMapping)
                        .build();
        return storage;
    }

    @Override
    public <I> JdbcRecordStorage<I> createRecordStorage(
            ContextSpec context, Class<? extends Entity<I, ?>> entityClass) {
        JdbcRecordStorage<I> recordStorage =
                JdbcRecordStorage.<I>newBuilder()
                        .setEntityClass(entityClass)
                        .setMultitenant(context.isMultitenant())
                        .setDataSource(dataSource)
                        .setColumnMapping(columnMapping)
                        .setTypeMapping(typeMapping)
                        .build();
        return recordStorage;
    }

    @Override
    public <I> ProjectionStorage<I> createProjectionStorage(
            ContextSpec context, Class<? extends Projection<I, ?, ?>> projectionClass) {
        JdbcRecordStorage<I> entityStorage = createRecordStorage(context, projectionClass);
        ProjectionStorage<I> storage = JdbcProjectionStorage.<I>newBuilder()
                .setMultitenant(context.isMultitenant())
                .setDataSource(dataSource)
                .setRecordStorage(entityStorage)
                .setProjectionClass(projectionClass)
                .setTypeMapping(typeMapping)
                .build();
        return storage;
    }

    @Override
    public InboxStorage createInboxStorage(boolean multitenant) {
        JdbcInboxStorage storage = JdbcInboxStorage
                .newBuilder()
                .setMultitenant(multitenant)
                .setDataSource(dataSource)
                .setTypeMapping(typeMapping)
                .build();
        return storage;
    }

    @Override
    public CatchUpStorage createCatchUpStorage(boolean multitenant) {
        JdbcCatchUpStorage storage = JdbcCatchUpStorage
                .newBuilder()
                .setMultitenant(multitenant)
                .setDataSource(dataSource)
                .setTypeMapping(typeMapping)
                .build();
        return storage;
    }

    /**
     * Creates a new storage for work session records based on JDBC.
     */
    public JdbcSessionStorage createSessionStorage() {
        return JdbcSessionStorage
                .newBuilder()
                .setDataSource(dataSource)
                .setTypeMapping(typeMapping)
                .build();
    }

    /**
     * Closes used {@link DataSourceWrapper}.
     */
    @Override
    public void close() {
        dataSource.close();
    }

    @VisibleForTesting
    TypeMapping getTypeMapping() {
        return typeMapping;
    }

    @VisibleForTesting
    JdbcColumnMapping<?> columnMapping() {
        return columnMapping;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builds instances of {@code JdbcStorageFactory}.
     */
    public static class Builder {

        private DataSourceWrapper dataSource;
        private JdbcColumnMapping<?> columnMapping;
        private TypeMapping typeMapping;

        private Builder() {
            // Prevent direct instantiation.
        }

        /**
         * Sets the {@linkplain io.spine.server.entity.storage.ColumnMapping column mapping} to use
         * in the generated storages.
         *
         * <p>The default value is a {@link DefaultJdbcColumnMapping}.
         *
         * @param columnMapping
         *         the column mapping to use in the generated storages
         */
        public Builder setColumnMapping(JdbcColumnMapping<?> columnMapping) {
            this.columnMapping = columnMapping;
            return this;
        }

        /**
         * Sets required field {@code dataSource}.
         */
        public Builder setDataSource(DataSourceWrapper dataSource) {
            this.dataSource = dataSource;
            return this;
        }

        /**
         * Sets required field {@code dataSource} from wrapped {@link DataSource}.
         *
         * @see DataSourceWrapper#wrap(DataSource)
         */
        public Builder setDataSource(DataSource dataSource) {
            this.dataSource = DataSourceWrapper.wrap(dataSource);
            return this;
        }

        /**
         * Sets required field {@code dataSource} from {@link DataSourceConfig}.
         *
         * @see HikariConfig
         * @see DefaultDataSourceConfigConverter#convert(DataSourceConfig)
         */
        public Builder setDataSource(DataSourceConfig dataSource) {
            HikariConfig hikariConfig = DefaultDataSourceConfigConverter.convert(dataSource);
            this.dataSource = DataSourceWrapper.wrap(new HikariDataSource(hikariConfig));
            return this;
        }

        /**
         * Sets {@link TypeMapping}, which defines {@link Type} names for the database used.
         *
         * <p>Use the {@linkplain TypeMappingBuilder#basicBuilder() basic builder}
         * to build a custom mapping.
         *
         * <p>If the mapping was not specified, it is
         * {@linkplain PredefinedMapping#select(DataSourceWrapper) selected} basing on
         * the {@linkplain java.sql.DatabaseMetaData#getDatabaseProductName() database product name}
         * and the database version.
         *
         * <p>If there is no mapping for the database,
         * {@linkplain PredefinedMapping#MYSQL_5_7 mapping for MySQL 5.7} is used.
         *
         * @param typeMapping
         *         the custom type mapping
         */
        public Builder setTypeMapping(TypeMapping typeMapping) {
            this.typeMapping = checkNotNull(typeMapping);
            return this;
        }

        /**
         * Returns a new instance of {@code JdbcStorageFactory}.
         */
        public JdbcStorageFactory build() {
            if (columnMapping == null) {
                columnMapping = new DefaultJdbcColumnMapping();
            }
            if (typeMapping == null) {
                typeMapping = PredefinedMapping.select(dataSource);
            }
            return new JdbcStorageFactory(this);
        }
    }
}
