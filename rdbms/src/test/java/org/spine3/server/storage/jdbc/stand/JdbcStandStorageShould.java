/*
 * Copyright 2016, TeamDev Ltd. All rights reserved.
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

package org.spine3.server.storage.jdbc.stand;

import org.junit.Test;
import org.spine3.protobuf.AnyPacker;
import org.spine3.protobuf.Timestamps;
import org.spine3.protobuf.TypeUrl;
import org.spine3.server.aggregate.Aggregate;
import org.spine3.server.stand.AggregateStateId;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.StandStorage;
import org.spine3.server.storage.jdbc.GivenDataSource;
import org.spine3.server.storage.jdbc.JdbcStandStorage;
import org.spine3.server.storage.jdbc.entity.query.CreateEntityTableQuery;
import org.spine3.server.storage.jdbc.entity.query.EntityStorageQueryFactory;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.test.aggregate.Project;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

/**
 * @author Dmytro Dashenkov
 */
public class JdbcStandStorageShould {

    /*
     * Initialize tests
     * ----------------
     */

    @SuppressWarnings("unchecked") // For mocks
    @Test
    public void initialize_properly_with_all_builder_fields() {
        final DataSourceWrapper dataSourceMock = mock(DataSourceWrapper.class);
        final EntityStorageQueryFactory<String> queryFactoryMock = (EntityStorageQueryFactory<String>) mock(EntityStorageQueryFactory.class);

        final CreateEntityTableQuery<String> queryMock = (CreateEntityTableQuery<String>) mock(CreateEntityTableQuery.class);
        when(queryFactoryMock.newCreateEntityTableQuery()).thenReturn(queryMock);
        doNothing().when(queryMock).execute();

        final StandStorage standStorage = JdbcStandStorage.<String>newBuilder()
                .setEntityStorageQueryFactory(queryFactoryMock)
                .setDataSource(dataSourceMock)
                .setMultitenant(false)
                .build();

        assertNotNull(standStorage);

        // Check table is created
        verify(queryFactoryMock).newCreateEntityTableQuery();
        verify(queryMock).execute();
    }


    @SuppressWarnings("unchecked") // For mocks
    @Test
    public void initialize_properly_without_multitenancy() {
        final DataSourceWrapper dataSourceMock = mock(DataSourceWrapper.class);
        final EntityStorageQueryFactory<String> queryFactoryMock = (EntityStorageQueryFactory<String>) mock(EntityStorageQueryFactory.class);

        final CreateEntityTableQuery<String> queryMock = (CreateEntityTableQuery<String>) mock(CreateEntityTableQuery.class);
        when(queryFactoryMock.newCreateEntityTableQuery()).thenReturn(queryMock);
        doNothing().when(queryMock).execute();

        final StandStorage standStorage = JdbcStandStorage.<String>newBuilder()
                .setEntityStorageQueryFactory(queryFactoryMock)
                .setDataSource(dataSourceMock)
                .build();

        assertNotNull(standStorage);
        assertFalse(standStorage.isMultitenant());
    }


    @Test(expected = NullPointerException.class)
    public void fail_to_initialize_with_empty_builder() {
        JdbcStandStorage.newBuilder().build();
    }


    @SuppressWarnings("unchecked") // For mocks
    @Test(expected = NullPointerException.class)
    public void fail_to_initialize_without_data_source() {
        final EntityStorageQueryFactory<String> queryFactoryMock = (EntityStorageQueryFactory<String>) mock(EntityStorageQueryFactory.class);

        final CreateEntityTableQuery<String> queryMock = (CreateEntityTableQuery<String>) mock(CreateEntityTableQuery.class);
        when(queryFactoryMock.newCreateEntityTableQuery()).thenReturn(queryMock);
        doNothing().when(queryMock).execute();

        JdbcStandStorage.<String>newBuilder()
                .setEntityStorageQueryFactory(queryFactoryMock)
                .setMultitenant(false)
                .build();
    }

    @Test(expected = NullPointerException.class)
    public void fail_to_initialize_without_query_factory() {
        final DataSourceWrapper dataSourceMock = mock(DataSourceWrapper.class);

        JdbcStandStorage.newBuilder()
                .setDataSource(dataSourceMock)
                .setMultitenant(false)
                .build();
    }

    /*
     * Read-write positive tests
     */

    @Test
    public void write_data_to_store() {
        final StandStorage storage = Given.newStorage();

        final Given.TestAggregate aggregate = new Given.TestAggregate("some_id");

        final AggregateStateId id = AggregateStateId.of(aggregate.getId(), TypeUrl.of(Project.class));
        final EntityStorageRecord record = EntityStorageRecord.newBuilder()
                .setState(AnyPacker.pack(aggregate.getState()))
                .setWhenModified(Timestamps.getCurrentTime())
                .setVersion(1)
                .build();

        storage.write(id, record);

        final EntityStorageRecord readRecord = storage.read(id);

        assertEquals(readRecord, record);
    }


    /*
     * Read-write negative tests
     */



    private static class Given {

        private static StandStorage newStorage() {
            final DataSourceWrapper dataSource = GivenDataSource.whichIsStoredInMemory(GivenDataSource.DEFAULT_TABLE_NAME);

            final EntityStorageQueryFactory<String> queryFactory = new EntityStorageQueryFactory<>(
                    dataSource,
                    TestAggregate.class);

            final StandStorage storage = JdbcStandStorage.<String>newBuilder()
                    .setEntityStorageQueryFactory(queryFactory)
                    .setDataSource(dataSource)
                    .build();

            return storage;
        }

        public static class TestAggregate extends Aggregate<String, Project, Project.Builder> {

            /**
             * Creates a new aggregate instance.
             *
             * @param id the ID for the new aggregate
             * @throws IllegalArgumentException if the ID is not of one of the supported types
             */
            public TestAggregate(String id) {
                super(id);
            }
        }
    }
}
