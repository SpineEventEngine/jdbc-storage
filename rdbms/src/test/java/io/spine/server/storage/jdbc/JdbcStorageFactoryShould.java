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
import org.junit.Test;

import javax.sql.DataSource;

import static io.spine.Identifier.newUuid;
import static io.spine.server.storage.jdbc.GivenDataSource.prefix;
import static io.spine.server.storage.jdbc.TypeMappings.mySql;
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
public class JdbcStorageFactoryShould {

    private final DataSourceConfig config = DataSourceConfig.newBuilder()
                                                            .setJdbcUrl(prefix("factoryTests"))
                                                            .setUsername("SA")
                                                            .setPassword("pwd")
                                                            .setMaxPoolSize(12)
                                                            .build();

    @Test
    public void allow_to_use_custom_data_source() {
        final JdbcStorageFactory factory = JdbcStorageFactory.newBuilder()
                                                             .setDataSource(mock(DataSource.class))
                                                             .setTypeMapping(mySql())
                                                             .build();

        assertNotNull(factory);
    }

    @Test
    public void create_multitenant_record_storage() {
        final JdbcStorageFactory factory = newFactory(true);
        final RecordStorage<String> storage = factory.createRecordStorage(TestEntity.class);
        assertTrue(storage.isMultitenant());
    }

    @Test
    public void create_singletenant_record_storage() {
        final JdbcStorageFactory factory = newFactory(false);
        final RecordStorage<String> storage = factory.createRecordStorage(TestEntity.class);
        assertFalse(storage.isMultitenant());
    }

    @Test
    public void create_multitenant_aggregate_storage() {
        final JdbcStorageFactory factory = newFactory(true);
        final AggregateStorage<String> storage =
                factory.createAggregateStorage(TestAggregate.class);
        assertTrue(storage.isMultitenant());
    }

    @Test
    public void create_singletenant_aggregate_storage() {
        final JdbcStorageFactory factory = newFactory(false);
        final AggregateStorage<String> storage =
                factory.createAggregateStorage(TestAggregate.class);
        assertFalse(storage.isMultitenant());
    }

    @Test
    public void create_multitenant_projection_storage() {
        final JdbcStorageFactory factory = newFactory(true);
        final ProjectionStorage<String> storage =
                factory.createProjectionStorage(TestProjection.class);
        assertTrue(storage.isMultitenant());
    }

    @Test
    public void create_singletenant_projection_storage() {
        final JdbcStorageFactory factory = newFactory(false);
        final ProjectionStorage<String> storage =
                factory.createProjectionStorage(TestProjection.class);
        assertFalse(storage.isMultitenant());
    }

    @Test
    public void create_multitenant_stand_storage() {
        final JdbcStorageFactory factory = newFactory(true);
        final StandStorage storage = factory.createStandStorage();
        assertTrue(storage.isMultitenant());
    }

    @Test
    public void create_singletenant_stand_storage() {
        final JdbcStorageFactory factory = newFactory(false);
        final StandStorage storage = factory.createStandStorage();
        assertFalse(storage.isMultitenant());
    }

    @Test
    public void close_datastore_on_close() {
        final DataSourceWrapper mock = GivenDataSource.withoutSuperpowers();
        final JdbcStorageFactory factory = JdbcStorageFactory.newBuilder()
                                                             .setDataSource(mock)
                                                             .setMultitenant(false)
                                                             .setTypeMapping(mySql())
                                                             .build();
        factory.close();
        verify(mock).close();
    }

    @Test
    public void have_default_column_type_registry() {
        final DataSourceWrapper dataSource = GivenDataSource.withoutSuperpowers();
        final JdbcStorageFactory factory = JdbcStorageFactory.newBuilder()
                                                             .setDataSource(dataSource)
                                                             .setTypeMapping(mySql())
                                                             .build();
        final ColumnTypeRegistry<?> registry = factory.getTypeRegistry();
        assertNotNull(registry);
    }

    @Test
    public void generate_single_tenant_view() {
        final DataSourceWrapper dataSource = GivenDataSource.withoutSuperpowers();
        final JdbcStorageFactory factory = JdbcStorageFactory.newBuilder()
                                                             .setMultitenant(true)
                                                             .setDataSource(dataSource)
                                                             .setTypeMapping(mySql())
                                                             .build();
        assertTrue(factory.isMultitenant());
        final JdbcStorageFactory singleTenantFactory = factory.toSingleTenant();
        assertFalse(singleTenantFactory.isMultitenant());
    }

    @Test
    public void use_self_as_single_tenant_view() {
        final DataSourceWrapper dataSource = GivenDataSource.withoutSuperpowers();
        final JdbcStorageFactory factory = JdbcStorageFactory.newBuilder()
                                                             .setMultitenant(false)
                                                             .setDataSource(dataSource)
                                                             .setTypeMapping(mySql())
                                                             .build();
        assertSame(factory, factory.toSingleTenant());
    }

    @Test
    public void use_MySQL_mapping_by_default() {
        final DataSourceWrapper dataSource = GivenDataSource.whichIsStoredInMemory(newUuid());
        final JdbcStorageFactory factory = JdbcStorageFactory.newBuilder()
                                                             .setMultitenant(false)
                                                             .setDataSource(dataSource)
                                                             .build();
        assertEquals(mySql(), factory.getTypeMapping());
    }

    private JdbcStorageFactory newFactory(boolean multitenant) {
        return JdbcStorageFactory.newBuilder()
                                 .setDataSource(config)
                                 .setMultitenant(multitenant)
                                 .setTypeMapping(mySql())
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
