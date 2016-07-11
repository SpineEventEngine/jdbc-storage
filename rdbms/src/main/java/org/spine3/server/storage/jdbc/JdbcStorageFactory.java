/*
 * Copyright 2016, TeamDev Ltd. All rights reserved.
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
import org.spine3.server.entity.Entity;
import org.spine3.server.storage.*;
import org.spine3.server.storage.jdbc.aggregate.JdbcAggregateStorage;
import org.spine3.server.storage.jdbc.aggregate.query.AggregateStorageQueryFactory;
import org.spine3.server.storage.jdbc.command.JdbcCommandStorage;
import org.spine3.server.storage.jdbc.command.query.CommandStorageQueryFactory;
import org.spine3.server.storage.jdbc.entity.JdbcEntityStorage;
import org.spine3.server.storage.jdbc.entity.query.EntityStorageQueryFactory;
import org.spine3.server.storage.jdbc.event.JdbcEventStorage;
import org.spine3.server.storage.jdbc.event.query.EventStorageQueryFactory;
import org.spine3.server.storage.jdbc.projection.JdbcProjectionStorage;
import org.spine3.server.storage.jdbc.projection.query.ProjectionStorageQueryFactory;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.DefaultDataSourceConfigConverter;

import javax.sql.DataSource;

/**
 * Creates storages based on JDBC-compliant RDBMS.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class JdbcStorageFactory implements StorageFactory {

    private final DataSourceWrapper dataSource;
    private final boolean multitenant;

    /**
     * Creates a new instance with the specified data source configuration.
     *
     * @param config        the config used to create the {@link DataSource}
     * @param multitenant   defines whether created storage will be multi-tenant
     */
    public static JdbcStorageFactory newInstance(DataSourceConfig config, boolean multitenant) {
        return new JdbcStorageFactory(config, multitenant);
    }

    /**
     * Creates a new instance with the specified data source.
     *
     * @param dataSource    the {@link DataSource} on which created storages are based.
     * @param multitenant   defines whether created storage will be multi-tenant
     */
    public static JdbcStorageFactory newInstance(DataSource dataSource, boolean multitenant) {
        return new JdbcStorageFactory(DataSourceWrapper.wrap(dataSource), multitenant);
    }

    protected JdbcStorageFactory(DataSourceConfig config, boolean multitenant) {
        final HikariConfig hikariConfig = DefaultDataSourceConfigConverter.convert(config);
        this.dataSource = DataSourceWrapper.wrap(new HikariDataSource(hikariConfig));
        this.multitenant = multitenant;
    }

    protected JdbcStorageFactory(DataSourceWrapper dataSource, boolean multitenant) {
        this.dataSource = dataSource;
        this.multitenant = multitenant;
    }

    @Override
    public boolean isMultitenant() {
        return multitenant;
    }

    @Override
    public CommandStorage createCommandStorage() {
        return JdbcCommandStorage.newInstance(dataSource, false, getCommandStorageQueryFactory(dataSource));
    }

    @Override
    public EventStorage createEventStorage() {
        return JdbcEventStorage.newInstance(dataSource, false, getEventStorageQueryFactory(dataSource));
    }

    @Override
    public <I> AggregateStorage<I> createAggregateStorage(Class<? extends Aggregate<I, ?, ?>> aggregateClass) {
        return JdbcAggregateStorage.newInstance(dataSource,
                false,
                getAggregateStorageQueryFactory(dataSource, aggregateClass));
    }

    @Override
    public <I> EntityStorage<I> createEntityStorage(Class<? extends Entity<I, ?>> entityClass) {
        return JdbcEntityStorage
                .newInstance(dataSource, false, getEntityStorageQueryFactory(dataSource, entityClass));
    }

    @Override
    public <I> ProjectionStorage<I> createProjectionStorage(Class<? extends Entity<I, ?>> projectionClass) {
        final JdbcEntityStorage<I> entityStorage = JdbcEntityStorage
                .newInstance(dataSource, false, getEntityStorageQueryFactory(dataSource, projectionClass));
        return JdbcProjectionStorage
                .newInstance(dataSource, entityStorage, false, getProjectionStorageQueryFactory(dataSource, projectionClass));
    }

    /**
     * Creates a new {@link AggregateStorageQueryFactory} which produces database queries for corresponding {@link JdbcAggregateStorage}.
     *
     * @param dataSource        {@link DataSource} on which corresponding {@link JdbcAggregateStorage} is based
     * @param aggregateClass    class of aggregates which are stored in the corresponding {@link JdbcAggregateStorage}
     * @param <I>               a type of IDs of stored aggregates
     */
    protected  <I> AggregateStorageQueryFactory<I> getAggregateStorageQueryFactory(DataSourceWrapper dataSource,
                                                                                   Class<? extends Aggregate<I, ?, ?>> aggregateClass){
        return new AggregateStorageQueryFactory<>(dataSource, aggregateClass);
    }

    /**
     * Creates a new {@link EntityStorageQueryFactory} which produces database queries for corresponding {@link JdbcEntityStorage}.
     *
     * @param dataSource        {@link DataSource} on which corresponding {@link JdbcEntityStorage} is based
     * @param entityClass       class of entities which are stored in the corresponding {@link JdbcEntityStorage}
     * @param <I>               a type of IDs of stored entities
     */
    protected <I> EntityStorageQueryFactory<I> getEntityStorageQueryFactory(DataSourceWrapper dataSource,
                                                                            Class<? extends Entity<I, ?>> entityClass){
        return new EntityStorageQueryFactory<>(dataSource, entityClass);
    }

    /**
     * Creates a new {@link ProjectionStorageQueryFactory} which produces database queries for corresponding {@link JdbcProjectionStorage}.
     *
     * @param dataSource        {@link DataSource} on which corresponding {@link JdbcProjectionStorage} is based
     * @param entityClass       class of entities which are stored in the corresponding {@link JdbcEntityStorage}
     * @param <I>               a type of IDs of entities from the corresponding {@link JdbcEntityStorage}
     */
    protected <I> ProjectionStorageQueryFactory<I> getProjectionStorageQueryFactory(DataSourceWrapper dataSource,
                                                                                    Class<? extends Entity<I, ?>> entityClass){
        return new ProjectionStorageQueryFactory<>(dataSource, entityClass);
    }

    /**
     * Creates a new {@link EventStorageQueryFactory} which produces database queries for corresponding {@link JdbcEventStorage}.
     *
     * @param dataSource        {@link DataSource} on which corresponding {@link JdbcEventStorage} is based
     */
    protected EventStorageQueryFactory getEventStorageQueryFactory(DataSourceWrapper dataSource){
        return new EventStorageQueryFactory(dataSource);
    }

    /**
     * Creates a new {@link CommandStorageQueryFactory} which produces database queries for corresponding {@link JdbcCommandStorage}.
     *
     * @param dataSource        {@link DataSource} on which corresponding {@link JdbcCommandStorage} is based
     */
    protected CommandStorageQueryFactory getCommandStorageQueryFactory(DataSourceWrapper dataSource){
        return new CommandStorageQueryFactory(dataSource);
    }

    @Override
    public void close() {
        dataSource.close();
    }

    protected DataSourceWrapper getDataSource() {
        return dataSource;
    }
}
