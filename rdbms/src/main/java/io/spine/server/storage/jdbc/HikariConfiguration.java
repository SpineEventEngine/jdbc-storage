/*
 * Copyright 2021, TeamDev. All rights reserved.
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

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Produces the {@link HikariConfig} from the generic {@link DataSourceConfig}.
 */
final class HikariConfiguration {

    /** Prevents this utility class from instantiation. */
    private HikariConfiguration() {
    }

    static HikariConfig from(DataSourceConfig config) {
        checkNotNull(config);
        var result = new HikariConfig();

        /* Required fields */

        result.setDataSourceClassName(config.getDataSourceClassName());
        result.setJdbcUrl(config.getJdbcUrl());
        result.setUsername(config.getUsername());
        result.setPassword(config.getPassword());

        /* Optional fields */

        autoCommit(config, result);
        connectionTimeout(config, result);
        idleTimeout(config, result);
        maxLifetime(config, result);
        connectionTestQuery(config, result);
        maxPoolSize(config, result);
        poolName(config, result);

        return result;
    }

    private static void poolName(DataSourceConfig config, HikariConfig result) {
        var poolName = config.getPoolName();
        if (poolName != null) {
            result.setPoolName(poolName);
        }
    }

    private static void maxPoolSize(DataSourceConfig config, HikariConfig result) {
        var maxPoolSize = config.getMaxPoolSize();
        if (maxPoolSize != null) {
            result.setMaximumPoolSize(maxPoolSize);
        }
    }

    private static void connectionTestQuery(DataSourceConfig config, HikariConfig result) {
        var connectionTestQuery = config.getConnectionTestQuery();
        if (connectionTestQuery != null) {
            result.setConnectionTestQuery(connectionTestQuery);
        }
    }

    private static void maxLifetime(DataSourceConfig config, HikariConfig result) {
        var maxLifetime = config.getMaxLifetime();
        if (maxLifetime != null) {
            result.setMaxLifetime(maxLifetime);
        }
    }

    private static void idleTimeout(DataSourceConfig config, HikariConfig result) {
        var idleTimeout = config.getIdleTimeout();
        if (idleTimeout != null) {
            result.setIdleTimeout(idleTimeout);
        }
    }

    private static void connectionTimeout(DataSourceConfig config, HikariConfig result) {
        var connectionTimeout = config.getConnectionTimeout();
        if (connectionTimeout != null) {
            result.setConnectionTimeout(connectionTimeout);
        }
    }

    private static void autoCommit(DataSourceConfig config, HikariConfig result) {
        var autoCommit = config.getAutoCommit();
        if (autoCommit != null) {
            result.setAutoCommit(autoCommit);
        }
    }
}
