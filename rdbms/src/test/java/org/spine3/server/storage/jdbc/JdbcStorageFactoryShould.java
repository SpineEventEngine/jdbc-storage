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

package org.spine3.server.storage.jdbc;

import com.google.protobuf.StringValue;
import org.junit.Before;
import org.junit.Test;
import org.spine3.server.aggregate.Aggregate;
import org.spine3.server.entity.Entity;
import org.spine3.server.projection.Projection;
import org.spine3.server.storage.AggregateStorage;
import org.spine3.server.storage.CommandStorage;
import org.spine3.server.storage.EventStorage;
import org.spine3.server.storage.ProjectionStorage;
import org.spine3.server.storage.RecordStorage;
import org.spine3.server.storage.StandStorage;
import org.spine3.test.storage.Project;

import javax.sql.DataSource;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

/**
 * @author Alexander Litus
 */
@SuppressWarnings({"InstanceMethodNamingConvention", "MagicNumber", "DuplicateStringLiteralInspection"})
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
        factory = JdbcStorageFactory.newBuilder()
                .setDataSource(config)
                .setMultitenant(false)
                .setEntityClass(TestAggregate.class)
                .setEntityStateDescriptor(StringValue.getDescriptor())
                .build();
    }

    @Test
    public void allow_to_use_custom_data_source(){
        final JdbcStorageFactory factory = JdbcStorageFactory.newBuilder()
                .setDataSource(mock(DataSource.class))
                .setEntityClass(TestProjection.class)
                .setEntityStateDescriptor(Project.getDescriptor())
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
        final AggregateStorage<String> storage = factory.createAggregateStorage(TestAggregate.class);
        assertNotNull(storage);
    }

    @Test
    public void create_event_storage() {
        final EventStorage storage = factory.createEventStorage();
        assertNotNull(storage);
    }

    @Test
    public void create_command_storage() {
        final CommandStorage storage = factory.createCommandStorage();
        assertNotNull(storage);
    }

    @Test
    public void create_projection_storage() {
        final ProjectionStorage<String> storage = factory.createProjectionStorage(TestProjection.class);
        assertNotNull(storage);
    }

    @Test
    public void create_stand_storage() {
        final StandStorage storage = factory.createStandStorage();
        assertNotNull(storage);
    }



    private static class TestEntity extends Entity<String, StringValue> {

        private TestEntity(String id) {
            super(id);
        }
    }

    private static class TestAggregate extends Aggregate<String, StringValue, StringValue.Builder> {

        private TestAggregate(String id) {
            super(id);
        }
    }

    private static class TestProjection extends Projection<String, Project> {

        private TestProjection(String id) {
            super(id);
        }
    }
}
