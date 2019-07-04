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

package io.spine.server.storage.jdbc;

import io.spine.server.aggregate.AggregateStorage;
import io.spine.server.projection.ProjectionStorage;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.TestAggregate;
import io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.TestEntity;
import io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.TestProjection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;

import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.jdbc.PredefinedMapping.MYSQL_5_7;
import static io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.multitenantSpec;
import static io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.newFactory;
import static io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.singletenantSpec;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@SuppressWarnings("DuplicateStringLiteralInspection") // Common test display names.
@DisplayName("JdbcStorageFactory should")
class JdbcStorageFactoryTest {

    @Test
    @DisplayName("allow to use custom data source")
    void allowCustomDataSource() {
        JdbcStorageFactory factory = JdbcStorageFactory
                .newBuilder()
                .setDataSource(mock(DataSource.class))
                .setTypeMapping(MYSQL_5_7)
                .build();

        assertNotNull(factory);
    }

    @Nested
    @DisplayName("create record storage")
    class CreateRecordStorage {

        @Test
        @DisplayName("which is multitenant")
        void multitenant() {
            JdbcStorageFactory factory = newFactory();
            RecordStorage<String> storage =
                    factory.createRecordStorage(multitenantSpec(), TestEntity.class);
            assertTrue(storage.isMultitenant());
        }

        @Test
        @DisplayName("which is single tenant")
        void singleTenant() {
            JdbcStorageFactory factory = newFactory();
            RecordStorage<String> storage =
                    factory.createRecordStorage(singletenantSpec(), TestEntity.class);
            assertFalse(storage.isMultitenant());
        }
    }

    @Nested
    @DisplayName("create aggregate storage")
    class CreateAggregateStorage {

        @Test
        @DisplayName("which is multitenant")
        void multitenant() {
            JdbcStorageFactory factory = newFactory();
            AggregateStorage<String> storage =
                    factory.createAggregateStorage(multitenantSpec(), TestAggregate.class);
            assertTrue(storage.isMultitenant());
        }

        @Test
        @DisplayName("which is single tenant")
        void singleTenant() {
            JdbcStorageFactory factory = newFactory();
            AggregateStorage<String> storage =
                    factory.createAggregateStorage(singletenantSpec(), TestAggregate.class);
            assertFalse(storage.isMultitenant());
        }
    }

    @Nested
    @DisplayName("create projection storage")
    class CreateProjectionStorage {

        @Test
        @DisplayName("which is multitenant")
        void multitenant() {
            JdbcStorageFactory factory = newFactory();
            ProjectionStorage<String> storage =
                    factory.createProjectionStorage(multitenantSpec(), TestProjection.class);
            assertTrue(storage.isMultitenant());
        }

        @Test
        @DisplayName("which is single tenant")
        void singleTenant() {
            JdbcStorageFactory factory = newFactory();
            ProjectionStorage<String> storage =
                    factory.createProjectionStorage(singletenantSpec(), TestProjection.class);
            assertFalse(storage.isMultitenant());
        }
    }

    @Test
    @DisplayName("close datastore on close")
    void closeDatastoreOnClose() {
        DataSourceWrapper mock = GivenDataSource.withoutSuperpowers();
        JdbcStorageFactory factory = JdbcStorageFactory
                .newBuilder()
                .setDataSource(mock)
                .setTypeMapping(MYSQL_5_7)
                .build();
        factory.close();
        verify(mock).close();
    }

    @Test
    @DisplayName("use MySQL mapping by default")
    void useMySqlMappingByDefault() {
        DataSourceWrapper dataSource = GivenDataSource.whichIsStoredInMemory(newUuid());
        JdbcStorageFactory factory = JdbcStorageFactory
                .newBuilder()
                .setDataSource(dataSource)
                .build();
        assertEquals(MYSQL_5_7, factory.getTypeMapping());
    }
}
