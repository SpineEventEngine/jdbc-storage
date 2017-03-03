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

package org.spine3.server.storage.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.spine3.server.aggregate.Aggregate;
import org.spine3.server.aggregate.AggregateStorage;
import org.spine3.server.command.CommandStorage;
import org.spine3.server.entity.Entity;
import org.spine3.server.event.EventStorage;
import org.spine3.server.projection.ProjectionStorage;
import org.spine3.server.stand.StandStorage;
import org.spine3.server.storage.RecordStorage;
import org.spine3.server.storage.StorageFactory;
import org.spine3.server.storage.jdbc.aggregate.JdbcAggregateStorage;
import org.spine3.server.storage.jdbc.aggregate.query.AggregateStorageQueryFactory;
import org.spine3.server.storage.jdbc.command.JdbcCommandStorage;
import org.spine3.server.storage.jdbc.command.query.CommandStorageQueryFactory;
import org.spine3.server.storage.jdbc.entity.JdbcRecordStorage;
import org.spine3.server.storage.jdbc.entity.query.RecordStorageQueryFactory;
import org.spine3.server.storage.jdbc.event.JdbcEventStorage;
import org.spine3.server.storage.jdbc.event.query.EventStorageQueryFactory;
import org.spine3.server.storage.jdbc.projection.JdbcProjectionStorage;
import org.spine3.server.storage.jdbc.projection.query.ProjectionStorageQueryFactory;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.DefaultDataSourceConfigConverter;

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
    public boolean isMultitenant() {
        return multitenant;
    }

    @Override
    public CommandStorage createCommandStorage() {
        final CommandStorageQueryFactory queryFactory = getCommandStorageQueryFactory(dataSource);
        final CommandStorage storage = JdbcCommandStorage.newBuilder()
                                                         .setQueryFactory(queryFactory)
                                                         .setDataSource(dataSource)
                                                         .setMultitenant(multitenant)
                                                         .build();
        return storage;
    }

    @Override
    public EventStorage createEventStorage() {
        final EventStorage storage = JdbcEventStorage.newBuilder()
                                                     .setDataSource(dataSource)
                                                     .setMultitenant(multitenant)
                                                     .build();
        return storage;
    }

    @Override
    public StandStorage createStandStorage() {
        final RecordStorageQueryFactory<I> recordStorageQueryFactory =
                getEntityStorageQueryFactory(dataSource, entityClass);
        return JdbcStandStorage.<I>newBuilder()
                               .setDataSource(dataSource)
                               .setMultitenant(isMultitenant())
                               .setEntityClass(entityClass)
                               .setQueryFactory(recordStorageQueryFactory)
                               .build();
    }

    @Override
    public <I> AggregateStorage<I> createAggregateStorage(
            Class<? extends Aggregate<I, ?, ?>> aggregateClass) {
        final AggregateStorageQueryFactory<I> queryFactory =
                getAggregateStorageQueryFactory(dataSource, aggregateClass);
        final JdbcAggregateStorage<I> storage = JdbcAggregateStorage.<I>newBuilder()
                                                                    .setQueryFactory(queryFactory)
                                                                    .setMultitenant(false)
                                                                    .setDataSource(dataSource)
                                                                    .build();
        return storage;
    }

    @Override
    public <I> RecordStorage<I> createRecordStorage(Class<? extends Entity<I, ?>> entityClass) {
        final RecordStorageQueryFactory<I> queryFactory = getEntityStorageQueryFactory(dataSource,
                                                                                       entityClass);
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
        final ProjectionStorageQueryFactory<I> queryFactory =
                getProjectionStorageQueryFactory(dataSource, projectionClass);

        final ProjectionStorage<I> storage = JdbcProjectionStorage.<I>newBuilder()
                                                                  .setMultitenant(multitenant)
                                                                  .setQueryFactory(queryFactory)
                                                                  .setDataSource(dataSource)
                                                                  .setRecordStorage(entityStorage)
                                                                  .build();
        return storage;
    }

    /**
     * Creates a new {@link AggregateStorageQueryFactory} which produces database queries for
     * corresponding {@link JdbcAggregateStorage}.
     *
     * @param dataSource     {@link DataSource} on which corresponding {@link JdbcAggregateStorage}
     *                                         is based
     * @param aggregateClass class of aggregates which are stored in the corresponding
     * {@link JdbcAggregateStorage}
     * @param <T>            a type of IDs of stored aggregates
     */
    protected <T> AggregateStorageQueryFactory<T> getAggregateStorageQueryFactory(
            DataSourceWrapper dataSource,
            Class<? extends Aggregate<T, ?, ?>> aggregateClass) {
        return new AggregateStorageQueryFactory<>(dataSource, aggregateClass);
    }

    /**
     * Creates a new {@link RecordStorageQueryFactory} which produces database queries for
     * corresponding {@link JdbcRecordStorage}.
     *
     * @param dataSource  {@link DataSource} on which corresponding {@link JdbcRecordStorage}
     *                                      is based
     * @param entityClass class of entities which are stored in the corresponding
     * {@link JdbcRecordStorage}
     * @param <T>         a type of IDs of stored entities
     */
    protected <T> RecordStorageQueryFactory<T> getEntityStorageQueryFactory(
            DataSourceWrapper dataSource,
            Class<? extends Entity<T, ?>> entityClass) {
        return new RecordStorageQueryFactory<>(dataSource, entityClass);
    }

    /**
     * Creates a new {@link ProjectionStorageQueryFactory} which produces database queries for
     * corresponding {@link JdbcProjectionStorage}.
     *
     * @param dataSource  {@link DataSource} on which corresponding {@link JdbcProjectionStorage}
     *                                      is based
     * @param entityClass class of entities which are stored in the corresponding
     * {@link JdbcRecordStorage}
     * @param <T>         a type of IDs of entities from the corresponding {@link JdbcRecordStorage}
     */
    protected <T> ProjectionStorageQueryFactory<T> getProjectionStorageQueryFactory(
            DataSourceWrapper dataSource,
            Class<? extends Entity<T, ?>> entityClass) {
        return new ProjectionStorageQueryFactory<>(dataSource);
    }

    /**
     * Creates a new {@link EventStorageQueryFactory} which produces database queries for
     * corresponding {@link JdbcEventStorage}.
     *
     * @param dataSource {@link DataSource} on which corresponding {@link JdbcEventStorage} is based
     */
    protected EventStorageQueryFactory getEventStorageQueryFactory(DataSourceWrapper dataSource) {
        return new EventStorageQueryFactory(dataSource);
    }

    /**
     * Creates a new {@link CommandStorageQueryFactory} which produces database queries for
     * corresponding {@link JdbcCommandStorage}.
     *
     * @param dataSource {@link DataSource} on which corresponding {@link JdbcCommandStorage}
     *                                     is based
     */
    protected CommandStorageQueryFactory getCommandStorageQueryFactory(
            DataSourceWrapper dataSource) {
        return new CommandStorageQueryFactory(dataSource);
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
