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
import io.spine.server.projection.ProjectionStorage;
import io.spine.server.stand.StandStorage;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.StorageFactory;
import io.spine.server.storage.jdbc.aggregate.JdbcAggregateStorage;
import io.spine.server.storage.jdbc.entity.JdbcRecordStorage;
import io.spine.server.storage.jdbc.projection.JdbcProjectionStorage;
import io.spine.server.storage.jdbc.util.DataSourceWrapper;
import io.spine.server.storage.jdbc.util.DefaultDataSourceConfigConverter;

import javax.sql.DataSource;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Creates storages based on JDBC-compliant RDBMS.
 *
 * @param <I> ID type of the {@link Entity} that will be stored in the storages created by
 *            this factory.
 * @author Alexander Litus
 * @author Andrey Lavrov
 * @author Dmytro Dashenkov
 */
public class JdbcStorageFactory<I> implements StorageFactory {

    private final DataSourceWrapper dataSource;
    private final boolean multitenant;
    private final Class<? extends Entity<I, ?>> entityClass;

    private JdbcStorageFactory(Builder<I> builder) {
        this.entityClass = checkNotNull(builder.entityClass);
        this.dataSource = checkNotNull(builder.dataSource);
        this.multitenant = builder.multitenant;
    }

    @Override
    public ColumnTypeRegistry getTypeRegistry() {
        return null;
    }

    @Override
    public StorageFactory toSingleTenant() {
        return null;
    }

    @Override
    public boolean isMultitenant() {
        return multitenant;
    }

    @Override
    public StandStorage createStandStorage() {
        return JdbcStandStorage.<I>newBuilder()
                .setDataSource(dataSource)
                .setMultitenant(isMultitenant())
                .setEntityClass(entityClass)
                .build();
    }

    @Override
    public <I> AggregateStorage<I> createAggregateStorage(
            Class<? extends Aggregate<I, ?, ?>> aggregateClass) {
        final JdbcAggregateStorage<I> storage =
                JdbcAggregateStorage.<I>newBuilder()
                        .setAggregateClass(aggregateClass)
                        .setMultitenant(false)
                        .setDataSource(dataSource)
                        .build();
        return storage;
    }

    @Override
    public <I> RecordStorage<I> createRecordStorage(Class<? extends Entity<I, ?>> entityClass) {
        final RecordStorage<I> recordStorage = JdbcRecordStorage.<I>newBuilder()
                .setMultitenant(false)
                .setEntityClass(entityClass)
                .setDataSource(dataSource)
                .build();
        return recordStorage;
    }

    @Override
    public <I> ProjectionStorage<I> createProjectionStorage(
            Class<? extends Entity<I, ?>> projectionClass) {
        final JdbcRecordStorage<I> entityStorage =
                (JdbcRecordStorage<I>) createRecordStorage(projectionClass);
        final ProjectionStorage<I> storage = JdbcProjectionStorage.<I>newBuilder()
                .setMultitenant(multitenant)
                .setDataSource(dataSource)
                .setRecordStorage(entityStorage)
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

    public static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    /**
     * Builds instances of {@code JdbcStorageFactory}.
     */
    public static class Builder<I> {

        private DataSourceWrapper dataSource;
        private boolean multitenant;
        private Class<? extends Entity<I, ?>> entityClass;

        private Builder() {
        }

        /**
         * Sets optional field {@code isMultitenant}. {@code false} is used by default.
         */
        public Builder<I> setMultitenant(boolean multitenant) {
            this.multitenant = multitenant;
            return this;
        }

        /**
         * Sets required field {@code entityClass}.
         */
        public Builder<I> setEntityClass(Class<? extends Entity<I, ?>> entityClass) {
            this.entityClass = entityClass;
            return this;
        }

        /**
         * Sets required field {@code dataSource}.
         */
        public Builder<I> setDataSource(DataSourceWrapper dataSource) {
            this.dataSource = dataSource;
            return this;
        }

        /**
         * Sets required field {@code dataSource} from wrapped {@link DataSource}.
         *
         * @see DataSourceWrapper#wrap(DataSource)
         */
        public Builder<I> setDataSource(DataSource dataSource) {
            this.dataSource = DataSourceWrapper.wrap(dataSource);
            return this;
        }

        /**
         * Sets required field {@code dataSource} from {@link DataSourceConfig}.
         *
         * @see HikariConfig
         * @see DefaultDataSourceConfigConverter#convert(DataSourceConfig)
         */
        public Builder<I> setDataSource(DataSourceConfig dataSource) {
            final HikariConfig hikariConfig = DefaultDataSourceConfigConverter.convert(dataSource);
            this.dataSource = DataSourceWrapper.wrap(new HikariDataSource(hikariConfig));
            return this;
        }

        /**
         * @return New instance of {@code JdbcStorageFactory}.
         */
        public JdbcStorageFactory<I> build() {
            return new JdbcStorageFactory<>(this);
        }

    }
}
