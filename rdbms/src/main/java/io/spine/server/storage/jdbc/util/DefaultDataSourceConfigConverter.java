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

package io.spine.server.storage.jdbc.util;

import com.zaxxer.hikari.HikariConfig;
import io.spine.server.storage.jdbc.DataSourceConfig;

import javax.sql.DataSource;

/**
 * A converter for the default {@link DataSource} config.
 * The default implementation is:
 * <a href="https://github.com/brettwooldridge/HikariCP">HikariCP</a> connection pool.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class DefaultDataSourceConfigConverter {

    private DefaultDataSourceConfigConverter() {
    }

    @SuppressWarnings("MethodWithMoreThanThreeNegations") // is OK in this case
    public static HikariConfig convert(DataSourceConfig config) {
        final HikariConfig result = new HikariConfig();

        /** Required fields */

        result.setDataSourceClassName(config.getDataSourceClassName());
        result.setJdbcUrl(config.getJdbcUrl());
        result.setUsername(config.getUsername());
        result.setPassword(config.getPassword());

        /** Optional fields */

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
