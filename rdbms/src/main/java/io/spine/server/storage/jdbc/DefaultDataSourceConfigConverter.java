/*
 * Copyright 2022, TeamDev. All rights reserved.
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

import com.zaxxer.hikari.HikariConfig;

import javax.sql.DataSource;

/**
 * A converter for the default {@link DataSource} config.
 *
 * <p>The default implementation is:
 * <a href="https://github.com/brettwooldridge/HikariCP">HikariCP</a> connection pool.
 */
class DefaultDataSourceConfigConverter {

    private DefaultDataSourceConfigConverter() {
        // Prevent utility class instantiation.
    }

    @SuppressWarnings("MethodWithMoreThanThreeNegations") // is OK in this case
    static HikariConfig convert(DataSourceConfig config) {
        HikariConfig result = new HikariConfig();

        /* Required fields */

        result.setDataSourceClassName(config.getDataSourceClassName());
        result.setJdbcUrl(config.getJdbcUrl());
        result.setUsername(config.getUsername());
        result.setPassword(config.getPassword());

        /* Optional fields */

        Boolean autoCommit = config.getAutoCommit();
        if (autoCommit != null) {
            result.setAutoCommit(autoCommit);
        }

        Long connectionTimeout = config.getConnectionTimeout();
        if (connectionTimeout != null) {
            result.setConnectionTimeout(connectionTimeout);
        }

        Long idleTimeout = config.getIdleTimeout();
        if (idleTimeout != null) {
            result.setIdleTimeout(idleTimeout);
        }

        Long maxLifetime = config.getMaxLifetime();
        if (maxLifetime != null) {
            result.setMaxLifetime(maxLifetime);
        }

        String connectionTestQuery = config.getConnectionTestQuery();
        if (connectionTestQuery != null) {
            result.setConnectionTestQuery(connectionTestQuery);
        }

        Integer maxPoolSize = config.getMaxPoolSize();
        if (maxPoolSize != null) {
            result.setMaximumPoolSize(maxPoolSize);
        }

        String poolName = config.getPoolName();
        if (poolName != null) {
            result.setPoolName(poolName);
        }
        return result;
    }
}
