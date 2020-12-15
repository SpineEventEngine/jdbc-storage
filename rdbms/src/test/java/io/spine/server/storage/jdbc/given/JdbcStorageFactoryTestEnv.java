/*
 * Copyright 2020, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.given;

import io.spine.server.ContextSpec;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.entity.storage.ColumnTypeMapping;
import io.spine.server.projection.Projection;
import io.spine.server.storage.jdbc.DataSourceConfig;
import io.spine.server.storage.jdbc.JdbcStorageFactory;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.type.JdbcColumnMapping;
import io.spine.test.storage.Project;

import static io.spine.server.storage.jdbc.GivenDataSource.prefix;
import static io.spine.server.storage.jdbc.PredefinedMapping.MYSQL_5_7;

public class JdbcStorageFactoryTestEnv {

    /** Prevents instantiation of this utility class. */
    private JdbcStorageFactoryTestEnv() {
    }

    private static final DataSourceConfig CONFIG = DataSourceConfig
            .newBuilder()
            .setJdbcUrl(prefix("factoryTests"))
            .setUsername("SA")
            .setPassword("pwd")
            .setMaxPoolSize(30)
            .build();

    public static JdbcStorageFactory newFactory() {
        return JdbcStorageFactory.newBuilder()
                                 .setDataSource(CONFIG)
                                 .setTypeMapping(MYSQL_5_7)
                                 .build();
    }

    public static ContextSpec multitenantSpec() {
        return ContextSpec.multitenant(JdbcStorageFactoryTestEnv.class.getName());
    }

    public static ContextSpec singletenantSpec() {
        return ContextSpec.singleTenant(JdbcStorageFactoryTestEnv.class.getName());
    }

    public static class TestAggregate extends Aggregate<String, Project, Project.Builder> {

        private TestAggregate(String id) {
            super(id);
        }
    }

    public static class TestProjection extends Projection<String, Project, Project.Builder> {

        private TestProjection(String id) {
            super(id);
        }
    }

    public static class TestColumnMapping implements JdbcColumnMapping<String> {

        @Override
        public Type typeOf(Class<?> columnType) {
            return Type.STRING;
        }

        @Override
        public <T> ColumnTypeMapping<T, ? extends String> of(Class<T> type) {
            return o -> "always-the-same-string";
        }

        @Override
        public ColumnTypeMapping<?, ? extends String> ofNull() {
            return o -> "the-null";
        }
    }
}
