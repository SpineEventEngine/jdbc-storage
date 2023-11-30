/*
 * Copyright 2023, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.mysql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.JdbcStorageFactory;
import io.spine.server.storage.jdbc.PredefinedMapping;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.testcontainers.containers.MySQLContainer;

import static io.spine.server.storage.jdbc.PredefinedMapping.MYSQL_5_7;

/**
 * Defines the common routines to use in tests against MySQL instance.
 */
final class MysqlTests {

    /**
     * Controls whether ALL tests in this package are enabled.
     *
     * <p>By default, i.e. when run on CI, the tests are disabled due to their slow nature.
     */
    static final boolean enabled = false;

    /**
     * Prevents instantiation of this utility.
     */
    private MysqlTests() {
        // Do nothing.
    }

    /**
     * Creates a new {@code DataSourceWrapper} around the passed instance of MySQL container.
     *
     * <p>The connections are pooled via HikariCP, with its default settings.
     */
    static DataSourceWrapper wrap(MySQLContainer<?> container) {
        var config = new HikariConfig();
        config.setJdbcUrl(container.getJdbcUrl());
        config.setUsername(container.getUsername());
        config.setPassword(container.getPassword());

        var hikariSource = new HikariDataSource(config);
        var dataSource = DataSourceWrapper.wrap(hikariSource);
        return dataSource;
    }

    /**
     * Returns a new instance of MySQL container.
     */
    static MySQLContainer<?> mysqlContainer() {
        return new MySQLContainer<>("mysql:5.7");
    }

    /**
     * Returns a type mapping compatible with the MySQL version
     * run via the {@linkplain #mysqlContainer() container}.
     */
    static PredefinedMapping mysqlMapping() {
        return MYSQL_5_7;
    }

    /**
     * Stops the container, if it is not {@code null}.
     */
    static void stop(@Nullable MySQLContainer<?> container) {
        if (null != container) {
            container.stop();
        }
    }

    static JdbcStorageFactory factoryConnectingTo(MySQLContainer<?> server) {
        var dataSource = wrap(server);
        var factory = JdbcStorageFactory.newBuilder()
                .setDataSource(dataSource)
                .setTypeMapping(mysqlMapping())
                .build();
        return factory;
    }
}
