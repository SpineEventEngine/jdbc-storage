/*
 * Copyright 2019, TeamDev. All rights reserved.
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

import com.google.protobuf.StringValue;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.entity.AbstractEntity;
import io.spine.server.projection.Projection;
import io.spine.server.storage.jdbc.DataSourceConfig;
import io.spine.server.storage.jdbc.JdbcStorageFactory;
import io.spine.test.storage.Project;

import static io.spine.server.storage.jdbc.GivenDataSource.prefix;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
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
            .setMaxPoolSize(12)
            .build();

    public static JdbcStorageFactory newFactory(boolean multitenant) {
        return JdbcStorageFactory.newBuilder()
                                 .setDataSource(CONFIG)
                                 .setMultitenant(multitenant)
                                 .setTypeMapping(MYSQL_5_7)
                                 .build();
    }

    public static class TestEntity extends AbstractEntity<String, StringValue> {

        private TestEntity(String id) {
            super(id);
        }
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
}
