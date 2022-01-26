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

package io.spine.server.storage.jdbc;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Message;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.spine.server.ContextSpec;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.storage.RecordSpec;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.StorageFactory;
import io.spine.server.storage.jdbc.record.JdbcTableSpec;
import io.spine.server.storage.jdbc.record.column.CustomColumns;
import io.spine.server.storage.jdbc.aggregate.AggregateEventRecordColumns;
import io.spine.server.storage.jdbc.delivery.JdbcSessionStorage;
import io.spine.server.storage.jdbc.record.NewRecordStorage;
import io.spine.server.storage.jdbc.operation.OperationFactory;
import io.spine.server.storage.jdbc.type.DefaultJdbcColumnMapping;
import io.spine.server.storage.jdbc.type.JdbcColumnMapping;

import javax.sql.DataSource;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.requireNonNull;

/**
 * Creates storages based on JDBC-compliant RDBMS.
 *
 * @see DataSourceConfig
 */
public class JdbcStorageFactory implements StorageFactory {

    private final DataSourceWrapper dataSource;
    private final JdbcColumnMapping columnMapping;
    private final TypeMapping typeMapping;
    private final OperationFactory operations;

    //TODO:2022-01-13:alex.tymchenko: move this to a package-local holder.
    private final ImmutableMap<Class<? extends Message>, JdbcTableSpec<?, ?>> tables;
    private final ImmutableMap<Class<? extends Message>, CustomColumns<?>> columnSpecs;

    private JdbcStorageFactory(Builder builder) {
        this.dataSource = checkNotNull(builder.dataSource);
        this.columnMapping = builder.columnMapping;
        this.typeMapping = checkNotNull(builder.typeMapping);
        this.operations = OperationFactory.with(dataSource, columnMapping, typeMapping);
        this.tables = ImmutableMap.copyOf(builder.tables);
        this.columnSpecs = ImmutableMap.copyOf(builder.customCols);
    }

    @Override
    public <I, R extends Message> RecordStorage<I, R>
    createRecordStorage(ContextSpec context, RecordSpec<I, R, ?> spec) {
        var result = new NewRecordStorage<>(context, spec, this);
        return result;
    }

    public JdbcSessionStorage createSessionStorage(ContextSpec context) {
        return new JdbcSessionStorage(context, this);
    }

    /**
     * Closes used {@link DataSourceWrapper}.
     */
    @Override
    public void close() {
        dataSource.close();
    }

    /**
     * Returns the type mapping configured for this factory.
     */
    public final TypeMapping typeMapping() {
        return typeMapping;
    }

    /**
     * Returns the operation factory for this storage.
     */
    public final OperationFactory operations() {
        return operations;
    }

    /**
     * Returns the column mapping set for this factory.
     */
    public final JdbcColumnMapping columnMapping() {
        return columnMapping;
    }

    //TODO:2021-12-23:alex.tymchenko: think of hiding this method into this package.
    public final DataSourceWrapper dataSource() {
        return dataSource;
    }

    public <I, R extends Message> JdbcTableSpec<I, R> tableSpecFor(RecordSpec<I, R, ?> record) {
        var recordType = record.storedType();
        if(!tables.containsKey(recordType)) {
            var specs = this.columnSpecs.get(recordType);
            @SuppressWarnings("unchecked")
            var cast = (CustomColumns<R>) specs;
            var spec = new JdbcTableSpec<>(record, columnMapping, cast);
            return spec;
        }
        @SuppressWarnings("unchecked")
        var result = (JdbcTableSpec<I, R>) tables.get(recordType);
        return requireNonNull(result);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builds instances of {@code JdbcStorageFactory}.
     */
    public static class Builder {

        private DataSourceWrapper dataSource;
        private JdbcColumnMapping columnMapping;
        private TypeMapping typeMapping;
        private final Map<Class<? extends Message>, JdbcTableSpec<?, ?>> tables = new HashMap<>();
        private final Map<Class<? extends Message>, CustomColumns<?>> customCols = new HashMap<>();

        private Builder() {
            // Prevent direct instantiation.
        }

        /**
         * Sets the {@linkplain io.spine.server.storage.ColumnMapping column mapping} to use
         * in the generated storages.
         *
         * <p>The default value is a {@link DefaultJdbcColumnMapping}.
         *
         * @param columnMapping
         *         the column mapping to use in the generated storages
         */
        public Builder setColumnMapping(JdbcColumnMapping columnMapping) {
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
            var hikariConfig = DefaultDataSourceConfigConverter.convert(dataSource);
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

//        //TODO:2022-01-11:alex.tymchenko: have the same configuration, but for `EntityClass`.
//        public <I, R extends Message> Builder
//        configureTable(Class<I> idType, Class<R> recordType, JdbcTableSpec.ColumnSpec<R>... columns) {
//            checkNotNull(recordType);
//            if(!tables.containsKey(recordType)) {
//                var spec = new JdbcTableSpec<>(idType, recordType, columnMapping);
//                tables.put(recordType, spec);
//            }
//
//            @SuppressWarnings("unchecked")  /* As per the contract above. */
//            var spec = (JdbcTableSpec<I, R>) tables.get(recordType);
//            fn.accept(spec);
//            return this;
//        }

        @CanIgnoreReturnValue
        public <R extends Message>
        Builder configureColumns(Class<R> recordType, CustomColumns<R> columns) {
            customCols.put(recordType, columns);
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

            configureSystemTables();

            return new JdbcStorageFactory(this);
        }

        private void configureSystemTables() {
//            configureColumns(InboxMessage.class, InboxColumns.instance());
            configureColumns(AggregateEventRecord.class, AggregateEventRecordColumns.instance());
        }
    }

    interface ConfigureTable<I, R extends Message> {

        void accept(JdbcTableSpec<I, R> tableSpec);
    }
}
