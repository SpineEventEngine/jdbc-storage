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

package io.spine.server.storage.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.aggregate.AggregateStorage;
import io.spine.server.entity.Entity;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.projection.Projection;
import io.spine.server.projection.ProjectionStorage;
import io.spine.server.stand.StandStorage;
import io.spine.server.storage.StorageFactory;
import io.spine.server.storage.jdbc.aggregate.JdbcAggregateStorage;
import io.spine.server.storage.jdbc.projection.JdbcProjectionStorage;
import io.spine.server.storage.jdbc.record.JdbcRecordStorage;
import io.spine.server.storage.jdbc.type.JdbcColumnType;
import io.spine.server.storage.jdbc.type.JdbcTypeRegistryFactory;

import javax.sql.DataSource;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Creates storages based on JDBC-compliant RDBMS.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 * @author Dmytro Dashenkov
 * @see DataSourceConfig
 * @see JdbcTypeRegistryFactory
 */
public class JdbcStorageFactory implements StorageFactory {

    private final DataSourceWrapper dataSource;
    private final boolean multitenant;
    private final ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>> columnTypeRegistry;

    private JdbcStorageFactory(Builder builder) {
        this.dataSource = checkNotNull(builder.dataSource);
        this.multitenant = builder.multitenant;
        this.columnTypeRegistry = builder.columnTypeRegistry;
    }

    @Override
    public ColumnTypeRegistry getTypeRegistry() {
        return columnTypeRegistry;
    }

    @Override
    public JdbcStorageFactory toSingleTenant() {
        if (isMultitenant()) {
            return newBuilder().setColumnTypeRegistry(columnTypeRegistry)
                               .setDataSource(dataSource)
                               .setMultitenant(false)
                               .build();
        } else {
            return this;
        }
    }

    @Override
    public boolean isMultitenant() {
        return multitenant;
    }

    @Override
    public StandStorage createStandStorage() {
        return JdbcStandStorage.newBuilder()
                               .setDataSource(dataSource)
                               .setMultitenant(isMultitenant())
                               .build();
    }

    @Override
    public <I> AggregateStorage<I> createAggregateStorage(
            Class<? extends Aggregate<I, ?, ?>> aggregateClass) {
        final JdbcAggregateStorage<I> storage =
                JdbcAggregateStorage.<I>newBuilder()
                                    .setAggregateClass(aggregateClass)
                                    .setMultitenant(multitenant)
                                    .setDataSource(dataSource)
                                    .build();
        return storage;
    }

    @Override
    public <I> JdbcRecordStorage<I> createRecordStorage(Class<? extends Entity<I, ?>> entityClass) {
        final JdbcRecordStorage<I> recordStorage =
                JdbcRecordStorage.<I>newBuilder()
                                 .setMultitenant(multitenant)
                                 .setEntityClass(entityClass)
                                 .setDataSource(dataSource)
                                 .setColumnTypeRegistry(columnTypeRegistry)
                                 .build();
        return recordStorage;
    }

    @Override
    public <I> ProjectionStorage<I> createProjectionStorage(
            Class<? extends Projection<I, ?, ?>> projectionClass) {
        final JdbcRecordStorage<I> entityStorage = createRecordStorage(projectionClass);
        final ProjectionStorage<I> storage = JdbcProjectionStorage.<I>newBuilder()
                                                                  .setMultitenant(multitenant)
                                                                  .setDataSource(dataSource)
                                                                  .setRecordStorage(entityStorage)
                                                                  .setProjectionClass(projectionClass)
                                                                  .build();
        return storage;
    }

    /**
     * Closes used {@link DataSourceWrapper}.
     */
    @Override
    public void close() {
        dataSource.close();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builds instances of {@code JdbcStorageFactory}.
     */
    public static class Builder {

        private DataSourceWrapper dataSource;
        private boolean multitenant;
        private ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>> columnTypeRegistry;

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
         * @param columnTypeRegistry the custom {@link ColumnTypeRegistry} to use in the generated
         *                           storages
         */
        public Builder setColumnTypeRegistry(
                ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>>
                        columnTypeRegistry) {
            this.columnTypeRegistry = columnTypeRegistry;
            return this;
        }

        /**
         * Sets optional field {@code isMultitenant}. {@code false} is used by default.
         */
        public Builder setMultitenant(boolean multitenant) {
            this.multitenant = multitenant;
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
            final HikariConfig hikariConfig = DefaultDataSourceConfigConverter.convert(dataSource);
            this.dataSource = DataSourceWrapper.wrap(new HikariDataSource(hikariConfig));
            return this;
        }

        /**
         * @return new instance of {@code JdbcStorageFactory}
         */
        public JdbcStorageFactory build() {
            if (columnTypeRegistry == null) {
                columnTypeRegistry = JdbcTypeRegistryFactory.defaultInstance();
            }
            return new JdbcStorageFactory(this);
        }
    }
}
