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

package io.spine.server.storage.jdbc;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Message;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.spine.annotation.Internal;
import io.spine.base.EntityState;
import io.spine.server.ContextSpec;
import io.spine.server.entity.Entity;
import io.spine.server.entity.storage.SpecScanner;
import io.spine.server.storage.RecordSpec;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.StorageFactory;
import io.spine.server.storage.jdbc.config.CreateOperationFactory;
import io.spine.server.storage.jdbc.config.TableSpecs;
import io.spine.server.storage.jdbc.delivery.JdbcSessionStorage;
import io.spine.server.storage.jdbc.operation.OperationFactory;
import io.spine.server.storage.jdbc.record.JdbcRecordStorage;
import io.spine.server.storage.jdbc.record.JdbcTableSpec;
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
    private final JdbcColumnMapping columnMapping;
    private final TypeMapping typeMapping;
    private final OperationFactory operations;
    private final TableSpecs tableSpecs;

    private JdbcStorageFactory(Builder builder) {
        this.dataSource = checkNotNull(builder.dataSource);
        this.columnMapping = builder.columnMapping;
        this.typeMapping = checkNotNull(builder.typeMapping);
        this.operations = builder.createOpFactory.apply(dataSource, typeMapping);
        this.tableSpecs = builder.tableSpecs.build();
    }

    /**
     * Creates a new storage for records.
     *
     * @param context
     *         the bounded context within which the storage is being configured
     * @param spec
     *         the record specification for the stored record
     * @param <I>
     *         type of the record identifiers
     * @param <R>
     *         type of the stored records
     * @return a new instance of the record storage
     */
    @Override
    public <I, R extends Message> RecordStorage<I, R>
    createRecordStorage(ContextSpec context, RecordSpec<I, R> spec) {
        var result = new JdbcRecordStorage<>(context, spec, this);
        return result;
    }

    /**
     * Returns an SQL statement which would allow to manually create an RDMBS table
     * corresponding to some Entity registered in a certain Bounded Context.
     *
     * @param contextSpec
     *         specification of the Bounded Context, in which Entity is registered
     * @param entityClass
     *         type of Entity
     * @param <I>
     *         Entity ID type
     * @param <S>
     *         Entity state type
     * @return SQL statement to create the corresponding table
     */
    public <I, S extends EntityState<I>, E extends Entity<I, S>>
    String tableCreationSql(ContextSpec contextSpec, Class<E> entityClass) {
        checkNotNull(contextSpec);
        checkNotNull(entityClass);

        var recordSpec = SpecScanner.scan(entityClass);
        var storage = new JdbcRecordStorage<>(contextSpec, recordSpec, this, false);
        var result = storage.tableCreationSql();
        return result;
    }

    /**
     * Creates a storage for the delivery work sessions.
     *
     * @param context
     *         the specification of the bounded context
     *         within which this storage is being created
     */
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
     * Returns the operation factory for this factory.
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

    /**
     * Returns the original data source, on top of which this factory was created.
     */
    @Internal
    public final DataSourceWrapper dataSource() {
        return dataSource;
    }

    /**
     * Returns the DB table specification for the passed record specification.
     *
     * <p>Takes into account the {@linkplain Builder#setCustomMapping(Class, JdbcColumnMapping)
     * custom mapping} and the {@linkplain Builder#setTableName(Class, String) custom table name}
     * set for the records of target type.
     *
     * @param spec
     *         record specification
     * @param <I>
     *         type of the identifiers of the described record
     * @param <R>
     *         type of the described record
     * @return a new instance of table specification
     */
    public <I, R extends Message> JdbcTableSpec<I, R> tableSpecFor(RecordSpec<I, R> spec) {
        var tableSpec = tableSpecs.specFor(spec, columnMapping);
        return tableSpec;
    }

    /**
     * Creates a new {@code Builder} for this factory.
     */
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
        private final TableSpecs.Builder tableSpecs = TableSpecs.newBuilder();
        private CreateOperationFactory createOpFactory;

        /**
         * Prevents this builder from a direct instantiation.
         *
         * @apiNote This method is made {@code protected} for the potential descendants
         *         of this {@code Builder} type.
         */
        protected Builder() {
        }

        /**
         * Sets the {@linkplain io.spine.server.storage.ColumnMapping column mapping} to use
         * in the generated storages.
         *
         * <p>The default value is a {@link JdbcColumnMapping}.
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
         * Sets required field {@code dataSource} from the wrapped {@link DataSource}.
         *
         * @see DataSourceWrapper#wrap(DataSource)
         */
        public Builder setDataSource(DataSource dataSource) {
            this.dataSource = DataSourceWrapper.wrap(dataSource);
            return this;
        }

        /**
         * Sets the required field {@code dataSource} from {@link DataSourceConfig}.
         *
         * @see HikariConfig
         * @see HikariConfiguration#from(DataSourceConfig)
         */
        public Builder setDataSource(DataSourceConfig dataSource) {
            var hikariConfig = HikariConfiguration.from(dataSource);
            this.dataSource = DataSourceWrapper.wrap(new HikariDataSource(hikariConfig));
            return this;
        }

        /**
         * Sets {@link TypeMapping}, which defines {@link Type} names for the database used.
         *
         * <p>Use the {@linkplain TypeMappingBuilder#mappingBuilder() corresponding builder}
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
         * Sets the custom DB table name for the table storing the records of the specified type.
         *
         * <p>The name previously set, if any, is replaced with this call.
         *
         * <p>The name cannot be blank.
         *
         * <p>In case no custom name is defined,
         * a {@linkplain io.spine.server.storage.jdbc.record.TableNames#of(Class) default name}
         * is used.
         *
         * @param recordType
         *         the type of the stored record
         * @param name
         *         the table name
         * @param <R>
         *         the type of the stored record
         * @return this instance of {@code Builder}
         */
        @CanIgnoreReturnValue
        public <R extends Message>
        Builder setTableName(Class<R> recordType, String name) {
            tableSpecs.setTableName(recordType, name);
            return this;
        }

        /**
         * Sets the custom column mapping for the table storing the records of the specified type.
         *
         * <p>The mapping previously set, if any, is replaced with this call.
         *
         * <p>In case no custom mapping is defined for some table,
         * a {@linkplain #setColumnMapping(JdbcColumnMapping) a factory-wide value} is used.
         *
         * @param recordType
         *         the type of the stored record
         * @param mapping
         *         the custom mapping
         * @param <R>
         *         the type of the stored record
         * @return this instance of {@code Builder}
         */
        @CanIgnoreReturnValue
        public <R extends Message>
        Builder setCustomMapping(Class<R> recordType, JdbcColumnMapping mapping) {
            tableSpecs.setMapping(recordType, mapping);
            return this;
        }

        /**
         * Overrides the factory of DB operations to use with the storage factory.
         *
         * <p>By default, the {@link OperationFactory} is used.
         *
         * @param fn
         *         the function to create the operation factory
         * @return this instance of {@code Builder}
         */
        @CanIgnoreReturnValue
        public Builder useOperationFactory(CreateOperationFactory fn) {
            this.createOpFactory = checkNotNull(fn);
            return this;
        }

        /**
         * Returns a new instance of {@code JdbcStorageFactory}.
         */
        public JdbcStorageFactory build() {
            configureDefaults();
            return new JdbcStorageFactory(this);
        }

        /**
         * Configures the default values for this storage factory.
         *
         * @apiNote This method is made {@code protected} for the potential descendants
         *         of this {@code Builder} type.
         */
        @SuppressWarnings("WeakerAccess")
        protected void configureDefaults() {
            if (columnMapping == null) {
                columnMapping = new JdbcColumnMapping();
            }
            if (typeMapping == null) {
                typeMapping = PredefinedMapping.select(dataSource);
            }
            if(createOpFactory == null) {
                createOpFactory = OperationFactory::new;
            }
        }
    }
}
