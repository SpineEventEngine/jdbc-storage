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

import io.spine.base.Time;
import io.spine.environment.Tests;
import io.spine.server.ServerEnvironment;
import io.spine.server.storage.StorageFactory;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.JdbcStorageFactory;
import io.spine.server.storage.jdbc.delivery.JdbcCatchUpSmokeTest;
import io.spine.testing.SlowTest;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.testcontainers.containers.MySQLContainer;

import static io.spine.server.storage.jdbc.mysql.MysqlTests.mysqlContainer;
import static io.spine.server.storage.jdbc.mysql.MysqlTests.mysqlMapping;
import static io.spine.server.storage.jdbc.mysql.MysqlTests.stop;
import static io.spine.server.storage.jdbc.mysql.MysqlTests.wrap;

@SlowTest
@DisplayName("JDBC-backed `CatchUp` on MySQL should")
@EnableConditionally
final class MysqlCatchUpSmokeTest extends JdbcCatchUpSmokeTest {

    private @Nullable MySQLContainer<?> mysql;
    private StorageFactory factory;

    @Override
    @BeforeEach
    public void setUp() {
        mysql = mysqlContainer();
        mysql.start();
        DataSourceWrapper dataSource = wrap(mysql);

        factory = JdbcStorageFactory
                .newBuilder()
                .setDataSource(dataSource)
                .setTypeMapping(mysqlMapping())
                .build();
        ServerEnvironment
                .when(Tests.class)
                .use(factory);
    }

    @AfterEach
    @Override
    public void tearDown() {
        Time.resetProvider();
        try {
            factory.close();
        } catch (Exception e) {
            throw new IllegalStateException("Error closing the MySQL-based storage factory", e);
        } finally {
            stop(mysql);
        }
    }
}
