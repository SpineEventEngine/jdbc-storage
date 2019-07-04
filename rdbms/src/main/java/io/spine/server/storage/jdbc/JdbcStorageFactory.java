/*
 * Copyright 2019, TeamDev. All rights reserved.
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
import com.google.common.collect.Maps;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.spine.core.TenantId;
import io.spine.server.ContextSpec;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.aggregate.AggregateStorage;
import io.spine.server.entity.Entity;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.projection.Projection;
import io.spine.server.projection.ProjectionStorage;
import io.spine.server.storage.StorageFactory;
import io.spine.server.storage.jdbc.aggregate.JdbcAggregateStorage;
import io.spine.server.storage.jdbc.projection.JdbcProjectionStorage;
import io.spine.server.storage.jdbc.record.JdbcRecordStorage;
import io.spine.server.storage.jdbc.type.JdbcColumnType;
import io.spine.server.storage.jdbc.type.JdbcTypeRegistryFactory;
import org.junit.jupiter.api.Disabled;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Creates storages based on JDBC-compliant RDBMS.
 *
 * @see DataSourceConfig
 * @see JdbcTypeRegistryFactory
 */
@Disabled
public class JdbcStorageFactory implements StorageFactory {

    // todo need to change this API
    private static final String TENANCY_TYPES_MIXED =
            "Use either `setDataSourcePerTenant` for multitenant environment or " +
            "`setDataSource` for the single tenant one";

    @Nullable
    private final DataSourceWrapper dataSource;

    @Nullable
    private final Map<TenantId, DataSourceWrapper> dataSourcePerTenant;

    private final Map<ContextSpec, DataSourceSupplier> supplierPerContext =
            Maps.newConcurrentMap();
    private final ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>> columnTypeRegistry;
    private final TypeMapping typeMapping;

    private JdbcStorageFactory(Builder builder) {
        this.dataSource = builder.dataSource;
        this.dataSourcePerTenant = builder.dataSourcePerTenant;
        this.columnTypeRegistry = builder.columnTypeRegistry;
        this.typeMapping = checkNotNull(builder.typeMapping);
    }

    @Override
    public <I> AggregateStorage<I> createAggregateStorage(
            ContextSpec context, Class<? extends Aggregate<I, ?, ?>> aggregateClass) {
        JdbcAggregateStorage<I> storage =
                JdbcAggregateStorage.<I>newBuilder()
                        .setAggregateClass(aggregateClass)
                        .setMultitenant(context.isMultitenant())
                        .setDataSourceSupplier(dataSourceSupplierFor(context))
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
                        .setDataSourceSupplier(dataSourceSupplierFor(context))
                        .setColumnTypeRegistry(columnTypeRegistry)
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
                .setDataSourceSupplier(dataSourceSupplierFor(context))
                .setRecordStorage(entityStorage)
                .setProjectionClass(projectionClass)
                .setTypeMapping(typeMapping)
                .build();
        return storage;
    }

    /**
     * Closes used {@link DataSourceWrapper}.
     */
    @Override
    public void close() {
        supplierPerContext.values().forEach(DataSourceSupplier::closeAll);
    }

    @VisibleForTesting
    TypeMapping getTypeMapping() {
        return typeMapping;
    }

    /**
     * Builds instances of {@code JdbcStorageFactory}.
     */
    private DataSourceSupplier dataSourceSupplierFor(ContextSpec context) {
        if (!supplierPerContext.containsKey(context)) {
            DataSourceSupplier supplier = createDataSourceSupplier(context);
            supplierPerContext.put(context, supplier);
        }
        return supplierPerContext.get(context);
    }

    private DataSourceSupplier createDataSourceSupplier(ContextSpec context) {
        if (context.isMultitenant()) {
            checkNotNull(dataSourcePerTenant);
            return new MultitenantDataSourceSupplier(dataSourcePerTenant);
        }
        // todo if we make user specify multiple data sources we somehow need to accept the default
        //  one for single tenant contexts.
        checkNotNull(dataSource);
        return new SingleTenantDataSourceSupplier(dataSource);
    }

    public static Builder newBuilder() {
        return new Builder();
    }
    public static class Builder {


        @Nullable
        private DataSourceWrapper dataSource;
        @Nullable
        private Map<TenantId, DataSourceWrapper> dataSourcePerTenant;
        private ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>> columnTypeRegistry;
        private TypeMapping typeMapping;

        private Builder() {
            // Prevent direct instantiation.
        }

        /**
         * Sets the {@link ColumnTypeRegistry} to use in the generated storages.
         *
         * <p>The default value is
         * {@link JdbcTypeRegistryFactory#defaultInstance() JdbcTypeRegistryFactory.defaultInstance()}.
         *
         * <p>To reuse the existent {@linkplain JdbcColumnType column types}, use
         * {@link JdbcTypeRegistryFactory#predefinedValuesAnd() JdbcTypeRegistryFactory.predefinedValuesAnd()}.
         *
         * @param columnTypeRegistry
         *         the custom {@link ColumnTypeRegistry} to use in the generated
         *         storages
         */
        public Builder setColumnTypeRegistry(
                ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>>
                        columnTypeRegistry) {
            this.columnTypeRegistry = columnTypeRegistry;
            return this;
        }

        /**
         * Sets required field {@code dataSource}.
         */
        public Builder setDataSource(DataSourceWrapper dataSource) {
            checkArgument(dataSourcePerTenant == null, TENANCY_TYPES_MIXED);
            this.dataSource = dataSource;
            return this;
        }

        /**
         * Sets required field {@code dataSource} from wrapped {@link DataSource}.
         *
         * @see DataSourceWrapper#wrap(DataSource)
         */
        public Builder setDataSource(DataSource dataSource) {
            checkArgument(dataSourcePerTenant == null, TENANCY_TYPES_MIXED);
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

        public Builder setDataSourcePerTenant(Map<TenantId, DataSourceConfig> dataSourcePerTenant) {
            checkArgument(dataSource == null, TENANCY_TYPES_MIXED);
            this.dataSourcePerTenant = new HashMap<>();
            dataSourcePerTenant.forEach((tenantId, config) -> {
                HikariConfig hikariConfig = DefaultDataSourceConfigConverter.convert(config);
                DataSourceWrapper wrapper =
                        DataSourceWrapper.wrap(new HikariDataSource(hikariConfig));
                this.dataSourcePerTenant.put(tenantId, wrapper);
            });
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
            if (columnTypeRegistry == null) {
                columnTypeRegistry = JdbcTypeRegistryFactory.defaultInstance();
            }
            if (typeMapping == null) {
                // todo store type mapping on per-data-source basis
                typeMapping = PredefinedMapping.select(dataSource);
            }
            return new JdbcStorageFactory(this);
        }
    }
}
