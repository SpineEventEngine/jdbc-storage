/*
 * Copyright 2026, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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
import io.spine.annotation.Experimental;
import io.spine.annotation.Internal;
import io.spine.base.EntityState;
import io.spine.core.BoundedContextName;
import io.spine.server.ContextSpec;
import io.spine.server.entity.Entity;
import io.spine.server.entity.storage.SpecScanner;
import io.spine.server.storage.RecordSpec;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.StorageFactory;
import io.spine.server.storage.StorageGroup;
import io.spine.server.storage.jdbc.config.CreateOperationFactory;
import io.spine.server.storage.jdbc.config.TableSpecs;
import io.spine.server.storage.jdbc.delivery.JdbcSessionStorage;
import io.spine.server.storage.jdbc.operation.OperationFactory;
import io.spine.server.storage.jdbc.record.JdbcRecordStorage;
import io.spine.server.storage.jdbc.record.JdbcTableSpec;
import io.spine.server.storage.jdbc.type.JdbcColumnMapping;
import org.jspecify.annotations.Nullable;

import javax.sql.DataSource;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Creates storages based on JDBC-compliant RDBMS.
 *
 * @see DataSourceConfig
 */
@Experimental
public class JdbcStorageFactory implements StorageFactory {

    /**
     * The wrapper over the JDBC data source against which the created storages operate.
     */
    private final DataSourceWrapper dataSource;

    /**
     * The factory-wide mapping of record columns to the columns of the DB tables,
     * unless overridden per table via {@link Builder#setCustomMapping}.
     */
    private final JdbcColumnMapping columnMapping;

    /**
     * The mapping of the generic SQL types to the dialect of the underlying DB engine.
     */
    private final TypeMapping typeMapping;

    /**
     * The factory of the low-level DB operations performed by the created storages.
     */
    private final OperationFactory operations;

    /**
     * The per-table settings made by the library user, along with the registry
     * of the table specifications created by this factory.
     */
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
     * <p>The records are stored in an RDBMS table, the identity of which is composed
     * of the passed record specification and the group. In particular, the storages
     * belonging to distinct groups are allocated their own tables, even if they store
     * records of the same type.
     *
     * @param context
     *         the bounded context within which the storage is being configured
     * @param spec
     *         the record specification for the stored record
     * @param group
     *         the group telling this storage apart from the other storages
     *         holding records of the same type,
     *         or {@code null} if the storage belongs to no particular group
     * @param <I>
     *         type of the record identifiers
     * @param <R>
     *         type of the stored records
     * @return a new instance of the record storage
     */
    @Override
    public <I, R extends Message> RecordStorage<I, R>
    createRecordStorage(ContextSpec context, RecordSpec<I, R> spec, @Nullable StorageGroup group) {
        var result = new JdbcRecordStorage<>(context, spec, this, group);
        return result;
    }

    /**
     * Returns an SQL statement that would allow manually creating an RDBMS table
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
     * Tells whether this storage factory is open for creating new storages.
     *
     * <p>The factory is open as long as its underlying {@link DataSourceWrapper} is not closed.
     */
    @Override
    public boolean isOpen() {
        return !dataSource.isClosed();
    }

    /**
     * Closes the used {@link DataSourceWrapper}.
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
     * Returns the DB table specification for the passed record specification,
     * for a storage belonging to no {@link StorageGroup}.
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
     * @return the table specification
     */
    public <I, R extends Message> JdbcTableSpec<I, R> tableSpecFor(RecordSpec<I, R> spec) {
        return tableSpecFor(spec, null);
    }

    /**
     * Returns the DB table specification for the passed record specification
     * and the storage group.
     *
     * <p>Takes into account the {@linkplain Builder#setCustomMapping(Class, JdbcColumnMapping)
     * custom mapping} set for the records of the target type.
     *
     * <p>For the storages belonging to no group, the
     * {@linkplain Builder#setTableName(Class, String) custom table name} is applied as well.
     * The tables of grouped storages take the custom names registered with
     * {@link Builder#setTableName(Class, Class, String)}; without one, they are named
     * after the {@linkplain io.spine.server.storage.jdbc.record.TableNames#of(Class,
     * StorageGroup) group and the record type}.
     *
     * @param spec
     *         record specification
     * @param group
     *         the group to which the storage belongs, or {@code null} if it belongs to none
     * @param <I>
     *         type of the identifiers of the described record
     * @param <R>
     *         type of the described record
     * @return the table specification
     */
    public <I, R extends Message> JdbcTableSpec<I, R>
    tableSpecFor(RecordSpec<I, R> spec, @Nullable StorageGroup group) {
        var tableSpec = tableSpecs.specFor(spec, group, columnMapping);
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
         * Sets the required field {@code dataSource}.
         */
        public Builder setDataSource(DataSourceWrapper dataSource) {
            this.dataSource = dataSource;
            return this;
        }

        /**
         * Sets the required field {@code dataSource} from the wrapped {@link DataSource}.
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
         * {@linkplain PredefinedMapping#select(DataSourceWrapper) selected} based on
         * the {@linkplain java.sql.DatabaseMetaData#getDatabaseProductName() database product name}
         * and the database version.
         *
         * <p>If there is no mapping for the database,
         * {@linkplain PredefinedMapping#MYSQL_9_7 mapping for MySQL 9.7} is used.
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
         * <p>For an Entity, pass the type of its state; for a standalone stored record,
         * such as {@code InboxMessage}, the type of the record itself.
         *
         * <p>The name previously set, if any, is replaced with this call.
         *
         * <p>The name cannot be blank.
         *
         * <p>In case no custom name is defined,
         * a {@linkplain io.spine.server.storage.jdbc.record.TableNames#of(Class) default name}
         * is used.
         *
         * <p>The custom name applies only to the storages belonging to no
         * {@link io.spine.server.storage.StorageGroup StorageGroup}. A custom name set
         * for an entity state type names the latest-state table alone, never the history
         * tables of that entity; use {@link #setTableName(Class, Class, String)} to name
         * the grouped tables.
         *
         * @param recordType
         *         the type of the stored record — for an Entity, its state type
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
         * Sets the custom DB table name for the table of a
         * {@linkplain io.spine.server.storage.StorageGroup grouped} storage serving
         * the entities with the specified state type — such as a per-entity history.
         *
         * <p>A grouped table is addressed by the storage group — named by the framework
         * after the entity state type — paired with the type of the stored records.
         * For instance, for the entities with the {@code Project} state:
         *
         * <pre>
         * // The event journal of the `Project` entities:
         * builder.setTableName(Project.class, Event.class, "project_journal");
         *
         * // The state history of the `Project` entities:
         * builder.setTableName(Project.class, EntityRecord.class, "project_state_history");
         * </pre>
         *
         * <p>The name previously set for the same grouped table, if any,
         * is replaced with this call.
         *
         * <p>The name cannot be blank.
         *
         * <p>In case no custom name is defined, a grouped table is
         * {@linkplain io.spine.server.storage.jdbc.record.TableNames#of(Class, StorageGroup)
         * named after the group and the record type}.
         *
         * <p>It is a responsibility of callers to select a name that does not collide
         * with the names of other tables, including the generated ones.
         *
         * @param stateType
         *         the type of the state of the entity served by the grouped storage
         * @param recordType
         *         the type of the records stored by the grouped storage
         * @param name
         *         the table name
         * @param <S>
         *         the type of the entity state
         * @param <R>
         *         the type of the stored record
         * @return this instance of {@code Builder}
         */
        @CanIgnoreReturnValue
        public <S extends EntityState<?>, R extends Message>
        Builder setTableName(Class<S> stateType, Class<R> recordType, String name) {
            tableSpecs.setTableName(stateType, recordType, name);
            return this;
        }

        /**
         * Sets the custom DB table name for the grouped table which serves
         * the Bounded Context with the given name, storing the records of
         * the specified type — such as the event store of the context.
         *
         * <p>A grouped table is addressed by the storage group — named by
         * the framework after the context, taking its name verbatim — paired
         * with the type of the stored records. For instance:
         *
         * <pre>
         * // The event store of the `Billing` Bounded Context:
         * builder.setTableName(BoundedContextNames.newName("Billing"), Event.class, "billing_events");
         * </pre>
         *
         * <p>To address the table of a System context, spell its name directly,
         * e.g. {@code BoundedContextNames.newName("Billing_System")}.
         *
         * <p>The name previously set for the same grouped table, if any,
         * is replaced with this call. The single-type
         * {@linkplain #setTableName(Class, String) custom names} never apply
         * to grouped tables.
         *
         * <p>The name cannot be blank.
         *
         * <p>It is the responsibility of callers to select a name that does not collide
         * with the names of other tables, including the generated ones.
         *
         * @param context
         *         the name of the Bounded Context served by the grouped storage
         * @param recordType
         *         the type of the records stored by the grouped storage
         * @param name
         *         the table name
         * @param <R>
         *         the type of the stored record
         * @return this instance of {@code Builder}
         */
        @CanIgnoreReturnValue
        public <R extends Message>
        Builder setTableName(BoundedContextName context, Class<R> recordType, String name) {
            tableSpecs.setTableName(context, recordType, name);
            return this;
        }

        /**
         * Sets the custom column mapping for the table storing the records of the specified type.
         *
         * <p>For an Entity, pass the type of its state; for a standalone stored record,
         * such as {@code InboxMessage}, the type of the record itself.
         *
         * <p>The mapping previously set, if any, is replaced with this call.
         *
         * <p>In case no custom mapping is defined for some table,
         * {@linkplain #setColumnMapping(JdbcColumnMapping) a factory-wide value} is used.
         *
         * <p>Unlike a {@linkplain #setTableName(Class, String) custom table name},
         * a custom mapping set for an entity state type also applies to the tables of
         * the {@linkplain io.spine.server.storage.StorageGroup grouped} storages serving
         * that entity, such as its state history.
         *
         * @param recordType
         *         the type of the stored record — for an Entity, its state type
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
