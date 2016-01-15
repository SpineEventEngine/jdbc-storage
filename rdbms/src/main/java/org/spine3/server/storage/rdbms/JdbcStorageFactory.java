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

package org.spine3.server.storage.rdbms;

import com.zaxxer.hikari.HikariConfig;
import org.spine3.server.Entity;
import org.spine3.server.aggregate.Aggregate;
import org.spine3.server.storage.*;

import javax.sql.DataSource;

/**
 * Creates storages based on JDBC-compliant RDBMS.
 *
 * @author Alexander Litus
 */
public class JdbcStorageFactory implements StorageFactory {

    private final DataSourceWrapper dataSource;

    /**
     * Creates a new instance with the specified data source configuration.
     *
     * @param config the config used to create the {@link DataSource}
     */
    public static JdbcStorageFactory newInstance(DataSourceConfig config) {
        return new JdbcStorageFactory(config);
    }

    private JdbcStorageFactory(DataSourceConfig config) {
        final HikariConfig hikariConfig = ConfigConverter.toHikariConfig(config);
        this.dataSource = HikariDataSourceWrapper.newInstance(hikariConfig);
    }

    @Override
    public CommandStorage createCommandStorage() {
        // TODO:2016-01-05:alexander.litus: impl
        return null;
    }

    @Override
    public EventStorage createEventStorage() {
        // TODO:2016-01-05:alexander.litus: impl
        return null;
    }

    /**
     * NOTE: the parameter is not used.
     */
    @Override
    public <I> AggregateStorage<I> createAggregateStorage(Class<? extends Aggregate<I, ?>> unused) {
        // TODO:2016-01-05:alexander.litus: impl
        return null;
    }

    @Override
    public <I> EntityStorage<I> createEntityStorage(Class<? extends Entity<I, ?>> entityClass) {
        return JdbcEntityStorage.newInstance(dataSource, entityClass);
    }

    @Override
    public void init() {
        // NOP
    }

    @Override
    public void close() {
        dataSource.close();
    }

    private static class ConfigConverter {

        @SuppressWarnings({"MethodWithMoreThanThreeNegations", "OverlyLongMethod"}) // is OK in this case
        private static HikariConfig toHikariConfig(DataSourceConfig config) {
            final HikariConfig result = new HikariConfig();

            // Required fields

            result.setDataSourceClassName(config.getDataSourceClassName());
            result.setJdbcUrl(config.getJdbcUrl());
            result.setUsername(config.getUsername());
            result.setPassword(config.getPassword());

            // Optional fields

            final Boolean autoCommit = config.getAutoCommit();
            if (autoCommit != null) {
                result.setAutoCommit(autoCommit);
            }

            final Long connectionTimeout = config.getConnectionTimeout();
            if (connectionTimeout != null) {
                result.setConnectionTimeout(connectionTimeout);
            }

            final Long idleTimeout = config.getIdleTimeout();
            if (idleTimeout != null) {
                result.setIdleTimeout(idleTimeout);
            }

            final Long maxLifetime = config.getMaxLifetime();
            if (maxLifetime != null) {
                result.setMaxLifetime(maxLifetime);
            }

            final String connectionTestQuery = config.getConnectionTestQuery();
            if (connectionTestQuery != null) {
                result.setConnectionTestQuery(connectionTestQuery);
            }

            final Integer maxPoolSize = config.getMaxPoolSize();
            if (maxPoolSize != null) {
                result.setMaximumPoolSize(maxPoolSize);
            }

            final String poolName = config.getPoolName();
            if (poolName != null) {
                result.setPoolName(poolName);
            }
            return result;
        }
    }
}
