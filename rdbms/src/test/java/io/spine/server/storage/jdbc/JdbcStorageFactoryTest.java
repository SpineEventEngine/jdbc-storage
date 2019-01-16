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
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.projection.ProjectionStorage;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.StorageFactory;
import io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.TestAggregate;
import io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.TestEntity;
import io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.TestProjection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;

import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.jdbc.GivenDataSource.prefix;
import static io.spine.server.storage.jdbc.PredefinedMapping.MYSQL_5_7;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Alexander Litus
 */
@SuppressWarnings("DuplicateStringLiteralInspection") // Common test display names.
@DisplayName("JdbcStorageFactory should")
class JdbcStorageFactoryTest {

    private final DataSourceConfig config = DataSourceConfig.newBuilder()
                                                            .setJdbcUrl(prefix("factoryTests"))
                                                            .setUsername("SA")
                                                            .setPassword("pwd")
                                                            .setMaxPoolSize(12)
                                                            .build();

    @Test
    @DisplayName("allow to use custom data source")
    void allowCustomDataSource() {
        JdbcStorageFactory factory = JdbcStorageFactory.newBuilder()
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
            JdbcStorageFactory factory = newFactory(true);
            RecordStorage<String> storage = factory.createRecordStorage(TestEntity.class);
            assertTrue(storage.isMultitenant());
        }

        @Test
        @DisplayName("which is single tenant")
        void singleTenant() {
            JdbcStorageFactory factory = newFactory(false);
            RecordStorage<String> storage = factory.createRecordStorage(TestEntity.class);
            assertFalse(storage.isMultitenant());
        }
    }

    @Nested
    @DisplayName("create aggregate storage")
    class CreateAggregateStorage {

        @Test
        @DisplayName("which is multitenant")
        void multitenant() {
            JdbcStorageFactory factory = newFactory(true);
            AggregateStorage<String> storage =
                    factory.createAggregateStorage(TestAggregate.class);
            assertTrue(storage.isMultitenant());
        }

        @Test
        @DisplayName("which is single tenant")
        void singleTenant() {
            JdbcStorageFactory factory = newFactory(false);
            AggregateStorage<String> storage =
                    factory.createAggregateStorage(TestAggregate.class);
            assertFalse(storage.isMultitenant());
        }
    }

    @Nested
    @DisplayName("create projection storage")
    class CreateProjectionStorage {

        @Test
        @DisplayName("which is multitenant")
        void multitenant() {
            JdbcStorageFactory factory = newFactory(true);
            ProjectionStorage<String> storage =
                    factory.createProjectionStorage(TestProjection.class);
            assertTrue(storage.isMultitenant());
        }

        @Test
        @DisplayName("which is single tenant")
        void singleTenant() {
            JdbcStorageFactory factory = newFactory(false);
            ProjectionStorage<String> storage =
                    factory.createProjectionStorage(TestProjection.class);
            assertFalse(storage.isMultitenant());
        }
    }

    @Test
    @DisplayName("close datastore on close")
    void closeDatastoreOnClose() {
        DataSourceWrapper mock = GivenDataSource.withoutSuperpowers();
        JdbcStorageFactory factory = JdbcStorageFactory.newBuilder()
                                                       .setDataSource(mock)
                                                       .setMultitenant(false)
                                                       .setTypeMapping(MYSQL_5_7)
                                                       .build();
        factory.close();
        verify(mock).close();
    }

    @Test
    @DisplayName("have default column type registry")
    void haveDefaultTypeRegistry() {
        DataSourceWrapper dataSource = GivenDataSource.withoutSuperpowers();
        JdbcStorageFactory factory = JdbcStorageFactory.newBuilder()
                                                       .setDataSource(dataSource)
                                                       .setTypeMapping(MYSQL_5_7)
                                                       .build();
        ColumnTypeRegistry<?> registry = factory.getTypeRegistry();
        assertNotNull(registry);
    }

    @Test
    @DisplayName("generate single tenant view")
    void generateSingleTenantView() {
        DataSourceWrapper dataSource = GivenDataSource.withoutSuperpowers();
        JdbcStorageFactory factory = JdbcStorageFactory.newBuilder()
                                                       .setMultitenant(true)
                                                       .setDataSource(dataSource)
                                                       .setTypeMapping(MYSQL_5_7)
                                                       .build();
        assertTrue(factory.isMultitenant());
        StorageFactory singleTenantFactory = factory.toSingleTenant();
        assertFalse(singleTenantFactory.isMultitenant());
    }

    @Test
    @DisplayName("use self as single tenant view")
    void useSelfAsSingleTenantView() {
        DataSourceWrapper dataSource = GivenDataSource.withoutSuperpowers();
        JdbcStorageFactory factory = JdbcStorageFactory.newBuilder()
                                                       .setMultitenant(false)
                                                       .setDataSource(dataSource)
                                                       .setTypeMapping(MYSQL_5_7)
                                                       .build();
        assertSame(factory, factory.toSingleTenant());
    }

    @Test
    @DisplayName("use MySQL mapping by default")
    void useMySqlMappingByDefault() {
        DataSourceWrapper dataSource = GivenDataSource.whichIsStoredInMemory(newUuid());
        JdbcStorageFactory factory = JdbcStorageFactory.newBuilder()
                                                       .setMultitenant(false)
                                                       .setDataSource(dataSource)
                                                       .build();
        assertEquals(MYSQL_5_7, factory.getTypeMapping());
    }

    private JdbcStorageFactory newFactory(boolean multitenant) {
        return JdbcStorageFactory.newBuilder()
                                 .setDataSource(config)
                                 .setMultitenant(multitenant)
                                 .setTypeMapping(MYSQL_5_7)
                                 .build();
    }
}
