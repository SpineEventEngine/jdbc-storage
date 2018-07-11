/*
 * Copyright 2018, TeamDev. All rights reserved.
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

import com.google.protobuf.StringValue;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.aggregate.AggregateStorage;
import io.spine.server.entity.AbstractEntity;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.projection.Projection;
import io.spine.server.projection.ProjectionStorage;
import io.spine.server.stand.StandStorage;
import io.spine.server.storage.RecordStorage;
import io.spine.test.storage.Project;
import io.spine.test.storage.ProjectVBuilder;
import io.spine.validate.StringValueVBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;

import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.jdbc.GivenDataSource.prefix;
import static io.spine.server.storage.jdbc.PredefinedMapping.MYSQL_5_7;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Alexander Litus
 */
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
    void allowToUseCustomDataSource() {
        final JdbcStorageFactory factory = JdbcStorageFactory.newBuilder()
                                                             .setDataSource(mock(DataSource.class))
                                                             .setTypeMapping(MYSQL_5_7)
                                                             .build();

        assertNotNull(factory);
    }

    @Test
    @DisplayName("create multitenant record storage")
    void createMultitenantRecordStorage() {
        final JdbcStorageFactory factory = newFactory(true);
        final RecordStorage<String> storage = factory.createRecordStorage(TestEntity.class);
        assertTrue(storage.isMultitenant());
    }

    @Test
    @DisplayName("create single tenant record storage")
    void createSingleTenantRecordStorage() {
        final JdbcStorageFactory factory = newFactory(false);
        final RecordStorage<String> storage = factory.createRecordStorage(TestEntity.class);
        assertFalse(storage.isMultitenant());
    }

    @Test
    @DisplayName("create multitenant aggregate storage")
    void createMultitenantAggregateStorage() {
        final JdbcStorageFactory factory = newFactory(true);
        final AggregateStorage<String> storage =
                factory.createAggregateStorage(TestAggregate.class);
        assertTrue(storage.isMultitenant());
    }

    @Test
    @DisplayName("create single tenant aggregate storage")
    void createSingleTenantAggregateStorage() {
        final JdbcStorageFactory factory = newFactory(false);
        final AggregateStorage<String> storage =
                factory.createAggregateStorage(TestAggregate.class);
        assertFalse(storage.isMultitenant());
    }

    @Test
    @DisplayName("create multitenant projection storage")
    void createMultitenantProjectionStorage() {
        final JdbcStorageFactory factory = newFactory(true);
        final ProjectionStorage<String> storage =
                factory.createProjectionStorage(TestProjection.class);
        assertTrue(storage.isMultitenant());
    }

    @Test
    @DisplayName("create single tenant projection storage")
    void createSingleTenantProjectionStorage() {
        final JdbcStorageFactory factory = newFactory(false);
        final ProjectionStorage<String> storage =
                factory.createProjectionStorage(TestProjection.class);
        assertFalse(storage.isMultitenant());
    }

    @Test
    @DisplayName("create multitenant stand storage")
    void createMultitenantStandStorage() {
        final JdbcStorageFactory factory = newFactory(true);
        final StandStorage storage = factory.createStandStorage();
        assertTrue(storage.isMultitenant());
    }

    @Test
    @DisplayName("create single tenant stand storage")
    void createSingleTenantStandStorage() {
        final JdbcStorageFactory factory = newFactory(false);
        final StandStorage storage = factory.createStandStorage();
        assertFalse(storage.isMultitenant());
    }

    @Test
    @DisplayName("close datastore on close")
    void closeDatastoreOnClose() {
        final DataSourceWrapper mock = GivenDataSource.withoutSuperpowers();
        final JdbcStorageFactory factory = JdbcStorageFactory.newBuilder()
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
        final DataSourceWrapper dataSource = GivenDataSource.withoutSuperpowers();
        final JdbcStorageFactory factory = JdbcStorageFactory.newBuilder()
                                                             .setDataSource(dataSource)
                                                             .setTypeMapping(MYSQL_5_7)
                                                             .build();
        final ColumnTypeRegistry<?> registry = factory.getTypeRegistry();
        assertNotNull(registry);
    }

    @Test
    @DisplayName("generate single tenant view")
    void generateSingleTenantView() {
        final DataSourceWrapper dataSource = GivenDataSource.withoutSuperpowers();
        final JdbcStorageFactory factory = JdbcStorageFactory.newBuilder()
                                                             .setMultitenant(true)
                                                             .setDataSource(dataSource)
                                                             .setTypeMapping(MYSQL_5_7)
                                                             .build();
        assertTrue(factory.isMultitenant());
        final JdbcStorageFactory singleTenantFactory = factory.toSingleTenant();
        assertFalse(singleTenantFactory.isMultitenant());
    }

    @Test
    @DisplayName("use self as single tenant view")
    void useSelfAsSingleTenantView() {
        final DataSourceWrapper dataSource = GivenDataSource.withoutSuperpowers();
        final JdbcStorageFactory factory = JdbcStorageFactory.newBuilder()
                                                             .setMultitenant(false)
                                                             .setDataSource(dataSource)
                                                             .setTypeMapping(MYSQL_5_7)
                                                             .build();
        assertSame(factory, factory.toSingleTenant());
    }

    @Test
    @DisplayName("use MySQL mapping by default")
    void useMySQLMappingByDefault() {
        final DataSourceWrapper dataSource = GivenDataSource.whichIsStoredInMemory(newUuid());
        final JdbcStorageFactory factory = JdbcStorageFactory.newBuilder()
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

    private static class TestEntity extends AbstractEntity<String, StringValue> {

        private TestEntity(String id) {
            super(id);
        }
    }

    private static class TestAggregate extends Aggregate<String, StringValue, StringValueVBuilder> {

        private TestAggregate(String id) {
            super(id);
        }
    }

    private static class TestProjection extends Projection<String, Project, ProjectVBuilder> {

        private TestProjection(String id) {
            super(id);
        }
    }
}
