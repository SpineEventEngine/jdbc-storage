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
import io.spine.server.projection.Projection;
import io.spine.server.projection.ProjectionStorage;
import io.spine.server.stand.StandStorage;
import io.spine.server.storage.RecordStorage;
import io.spine.test.storage.Project;
import io.spine.test.storage.ProjectVBuilder;
import io.spine.validate.StringValueVBuilder;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Alexander Litus
 */
@SuppressWarnings({"InstanceMethodNamingConvention", "DuplicateStringLiteralInspection"})
public class JdbcStorageFactoryShould {

    private JdbcStorageFactory factory;

    @Before
    public void setUpTest() {
        final String dbUrl = GivenDataSource.prefix("factoryTests");
        final DataSourceConfig config = DataSourceConfig.newBuilder()
                                                        .setJdbcUrl(dbUrl)
                                                        .setUsername("SA")
                                                        .setPassword("pwd")
                                                        .setMaxPoolSize(12)
                                                        .build();
        factory = JdbcStorageFactory.<String>newBuilder()
                                    .setDataSource(config)
                                    .setMultitenant(false)
                                    .build();
    }

    @Test
    public void allow_to_use_custom_data_source() {
        final JdbcStorageFactory factory = JdbcStorageFactory.<String>newBuilder()
                                                             .setDataSource(mock(DataSource.class))
                                                             .build();

        assertNotNull(factory);
    }

    @Test
    public void create_record_storage() {
        final RecordStorage<String> storage = factory.createRecordStorage(TestEntity.class);
        assertNotNull(storage);
    }

    @Test
    public void create_aggregate_storage() {
        final AggregateStorage<String> storage = factory.createAggregateStorage(
                TestAggregate.class);
        assertNotNull(storage);
    }


    @Test
    public void create_projection_storage() {
        final ProjectionStorage<String> storage = factory.createProjectionStorage(
                TestProjection.class);
        assertNotNull(storage);
    }

    @Test
    public void create_stand_storage() {
        final StandStorage storage = factory.createStandStorage();
        assertNotNull(storage);
    }

    @Test
    public void close_datastore_on_close() {
        final DataSourceWrapper mock = GivenDataSource.withoutSuperpowers();
        factory = JdbcStorageFactory.<String>newBuilder()
                                    .setDataSource(mock)
                                    .setMultitenant(false)
//                                    .setEntityClass(TestAggregate.class)
                                    .build();
        factory.close();
        verify(mock).close();
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
