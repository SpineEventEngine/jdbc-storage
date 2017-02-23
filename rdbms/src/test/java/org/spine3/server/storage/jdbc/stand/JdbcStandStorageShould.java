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

package org.spine3.server.storage.jdbc.stand;

import com.google.common.base.Optional;
import com.google.protobuf.Descriptors;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import org.junit.Test;
import org.spine3.protobuf.AnyPacker;
import org.spine3.protobuf.Timestamps2;
import org.spine3.protobuf.TypeUrl;
import org.spine3.server.aggregate.Aggregate;
import org.spine3.server.entity.AbstractEntity;
import org.spine3.server.entity.EntityRecord;
import org.spine3.server.stand.AggregateStateId;
import org.spine3.server.stand.StandStorage;
import org.spine3.server.stand.StandStorageShould;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.GivenDataSource;
import org.spine3.server.storage.jdbc.JdbcStandStorage;
import org.spine3.server.storage.jdbc.entity.query.CreateEntityTableQuery;
import org.spine3.server.storage.jdbc.entity.query.RecordStorageQueryFactory;
import org.spine3.server.storage.jdbc.entity.status.query.CreateVisibilityTableQuery;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.test.commandservice.customer.Customer;
import org.spine3.test.storage.Project;
import org.spine3.test.storage.ProjectId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.spine3.test.Verify.assertContains;
import static org.spine3.test.Verify.assertSize;

/**
 * @author Dmytro Dashenkov
 */
public class JdbcStandStorageShould extends StandStorageShould {

    @Override
    protected StandStorage getStorage() {
        final DataSourceWrapper dataSource = GivenDataSource.whichIsStoredInMemory(
                "StandStorageTests");
        return JdbcStandStorage.newBuilder()
                               .setDataSource(dataSource)
                               .setRecordStorageQueryFactory(
                                       new RecordStorageQueryFactory<>(dataSource,
                                                                       StandStorageRecord.class))
                               .setMultitenant(false)
                               .build();
    }

    /*
     * Initialize tests
     * ----------------
     */

    @SuppressWarnings("unchecked") // For mocks
    @Test
    public void initialize_properly_with_all_builder_fields() {
        final DataSourceWrapper dataSourceMock = mock(DataSourceWrapper.class);
        final RecordStorageQueryFactory<String> queryFactoryMock = (RecordStorageQueryFactory<String>) mock(
                RecordStorageQueryFactory.class);

        final CreateEntityTableQuery<String> queryMock = (CreateEntityTableQuery<String>) mock(
                CreateEntityTableQuery.class);
        final CreateVisibilityTableQuery statusQueryMock = mock(
                CreateVisibilityTableQuery.class);

        when(queryFactoryMock.newCreateEntityTableQuery()).thenReturn(queryMock);
        when(queryFactoryMock.newCreateVisibilityTableQuery()).thenReturn(statusQueryMock);
        doNothing().when(queryMock)
                   .execute();
        doNothing().when(statusQueryMock)
                   .execute();

        final StandStorage standStorage = JdbcStandStorage.<String>newBuilder()
                .setRecordStorageQueryFactory(queryFactoryMock)
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
        final RecordStorageQueryFactory<String> queryFactoryMock = (RecordStorageQueryFactory<String>) mock(
                RecordStorageQueryFactory.class);

        final CreateEntityTableQuery<String> queryMock = (CreateEntityTableQuery<String>) mock(
                CreateEntityTableQuery.class);
        final CreateVisibilityTableQuery statusQueryMock = mock(
                CreateVisibilityTableQuery.class);
        when(queryFactoryMock.newCreateEntityTableQuery()).thenReturn(queryMock);
        when(queryFactoryMock.newCreateVisibilityTableQuery()).thenReturn(statusQueryMock);
        doNothing().when(queryMock)
                   .execute();
        doNothing().when(statusQueryMock)
                   .execute();

        final StandStorage standStorage = JdbcStandStorage.<String>newBuilder()
                .setRecordStorageQueryFactory(queryFactoryMock)
                .setDataSource(dataSourceMock)
                .build();

        assertNotNull(standStorage);
        assertFalse(standStorage.isMultitenant());
    }

    @Test(expected = NullPointerException.class)
    public void fail_to_initialize_with_empty_builder() {
        JdbcStandStorage.newBuilder()
                        .build();
    }

    @SuppressWarnings("unchecked") // For mocks
    @Test(expected = NullPointerException.class)
    public void fail_to_initialize_without_data_source() {
        final RecordStorageQueryFactory<String> queryFactoryMock = (RecordStorageQueryFactory<String>) mock(
                RecordStorageQueryFactory.class);

        final CreateEntityTableQuery<String> queryMock = (CreateEntityTableQuery<String>) mock(
                CreateEntityTableQuery.class);
        when(queryFactoryMock.newCreateEntityTableQuery()).thenReturn(queryMock);
        doNothing().when(queryMock)
                   .execute();

        JdbcStandStorage.<String>newBuilder()
                .setRecordStorageQueryFactory(queryFactoryMock)
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
     * -------------------------
     */

    @Test
    public void write_data_to_store() {
        final StandStorage storage = Given.newStorage();

        final Given.TestAggregate aggregate = new Given.TestAggregate("some_id");

        final EntityRecord record = writeToStorage(aggregate, storage, Project.class);

        final Optional<EntityRecord> readRecord = storage.read(
                AggregateStateId.of(aggregate.getId(),
                                    TypeUrl.of(Project.class)));
        assertTrue(readRecord.isPresent());
        final EntityRecord actualRecord = readRecord.get();
        assertEquals(actualRecord, record);
    }

    @Test
    public void perform_bulk_read_operations() {
        final StandStorage storage = Given.newStorage();

        final Collection<Given.TestAggregate> testData = Given.testAggregates(10);

        final List<EntityRecord> records = new ArrayList<>();

        for (Aggregate aggregate : testData) {
            records.add(writeToStorage(aggregate, storage, Project.class));
        }

        final TypeUrl typeUrl = TypeUrl.of(Project.class);
        final Collection<AggregateStateId> ids = new LinkedList<>();
        ids.add(AggregateStateId.of("1", typeUrl));
        ids.add(AggregateStateId.of("2", typeUrl));
        ids.add(AggregateStateId.of("3", typeUrl));
        ids.add(AggregateStateId.of("5", typeUrl));
        ids.add(AggregateStateId.of("8", typeUrl));

        final Collection<EntityRecord> readRecords = (Collection<EntityRecord>) storage.readMultiple(
                ids);
        assertEquals(ids.size(), readRecords.size());

        assertContains(records.get(1), readRecords);
        assertContains(records.get(2), readRecords);
        assertContains(records.get(3), readRecords);
        assertContains(records.get(5), readRecords);
        assertContains(records.get(8), readRecords);
    }

    @Test
    public void handle_wrong_ids_silently() {
        final StandStorage storage = Given.newStorage();

        final TypeUrl typeUrl = TypeUrl.of(Project.class);
        final String repeatingInvalidId = "invalid-id-1";

        final Collection<AggregateStateId> ids = new LinkedList<>();
        ids.add(AggregateStateId.of(repeatingInvalidId, typeUrl));
        ids.add(AggregateStateId.of("invalid-id-2", typeUrl));
        ids.add(AggregateStateId.of(repeatingInvalidId, typeUrl));

        final Collection<EntityRecord> records = (Collection<EntityRecord>) storage.readMultiple(
                ids);

        assertNotNull(records);
        assertSize(0, records);
    }

    @SuppressWarnings("MethodWithMultipleLoops")
    @Test
    public void read_all_from_database() {
        final StandStorage storage = Given.newStorage();

        final Collection<Given.TestAggregate> testData = Given.testAggregates(10);

        final List<EntityRecord> records = new ArrayList<>();

        for (Aggregate aggregate : testData) {
            records.add(writeToStorage(aggregate, storage, Project.class));
        }

        final Map<AggregateStateId, EntityRecord> readRecords = storage.readAll();
        assertEquals(records.size(), readRecords.size());

        final Collection<EntityRecord> readValues = readRecords.values();
        for (EntityRecord record : records) {
            assertContains(record, readValues);
        }
    }

    @SuppressWarnings("DuplicateStringLiteralInspection") // Some field paths may repeat
    @Test
    public void apply_field_mask_to_read_values() {
        final StandStorage storage = Given.newStorage();

        final String stringId = "42";
        final AggregateStateId id = AggregateStateId.of(stringId, TypeUrl.of(Project.class));

        final Project project = Project.newBuilder()
                                       .setId(ProjectId.newBuilder()
                                                       .setId(stringId))
                                       .setName("Some name")
                                       .setStatus(Project.Status.DONE)
                                       .build();

        final Given.TestAggregate aggregate = new Given.TestAggregate(stringId);
        aggregate.setState(project);

        writeToStorage(aggregate, storage, Project.class);

        final List<Descriptors.FieldDescriptor> fields = Project.getDescriptor()
                                                                .getFields();
        final FieldMask idOnly = FieldMask.newBuilder()
                                          .addPaths(fields.get(0)
                                                          .getFullName())
                                          .build();
        final FieldMask idAndName = FieldMask.newBuilder()
                                             .addPaths(fields.get(0)
                                                             .getFullName())
                                             .addPaths(fields.get(1)
                                                             .getFullName())
                                             .build();
        final FieldMask nameAndStatus = FieldMask.newBuilder()
                                                 .addPaths(fields.get(1)
                                                                 .getFullName())
                                                 .addPaths(fields.get(3)
                                                                 .getFullName())
                                                 .build();

        final Optional<EntityRecord> recordOptional = storage.read(id, idOnly);
        assertTrue(recordOptional.isPresent());
        final EntityRecord record = recordOptional.get();
        final Project withIdOnly = AnyPacker.unpack(record.getState());
        final Project withIdAndName = AnyPacker.unpack(
                storage.readMultiple(Collections.singleton(id), idAndName)
                       .iterator()
                       .next()
                       .getState());
        final Project withNameAndStatus = AnyPacker.unpack(storage.readAll(nameAndStatus)
                                                                  .get(id)
                                                                  .getState());

        assertMatches(withIdOnly, idOnly);
        assertMatches(withIdAndName, idAndName);
        assertMatches(withNameAndStatus, nameAndStatus);
    }

    @Test
    public void read_all_by_type_url() {
        final StandStorage storage = Given.newStorage();

        final int aggregatesCount = 5;
        final List<Given.TestAggregate> aggregates = Given.testAggregates(aggregatesCount);
        final Given.TestAggregate2 differentAggregate = new Given.TestAggregate2("i_am_different");

        for (Aggregate aggregate : aggregates) {
            writeToStorage(aggregate, storage, Project.class);
        }

        final EntityRecord differentRecord = writeToStorage(differentAggregate, storage,
                                                                   Customer.class);

        final Collection<EntityRecord> records = storage.readAllByType(
                TypeUrl.of(Project.class));
        assertSize(aggregatesCount, records);
        assertFalse(records.contains(differentRecord));
    }

    @SuppressWarnings("MethodWithMultipleLoops")
    @Test
    public void read_by_type_and_apply_field_mask() {
        final StandStorage storage = Given.newStorage();

        final List<Given.TestAggregate> aggregates = Given.testAggregatesWithState(5);

        for (Aggregate aggregate : aggregates) {
            writeToStorage(aggregate, storage, Project.class);
        }

        final FieldMask namesMask = FieldMask.newBuilder()
                                             .addPaths(Project.getDescriptor()
                                                              .getFields()
                                                              .get(1)
                                                              .getFullName())
                                             .build();

        final Collection<EntityRecord> records = storage.readAllByType(
                TypeUrl.of(Project.class), namesMask);

        for (EntityRecord record : records) {
            final Project project = AnyPacker.unpack(record.getState());
            assertMatches(project, namesMask);
        }
    }

    /*
     * Read-write negative tests
     * -------------------------
     */

    @Test(expected = DatabaseException.class)
    public void fail_to_fetch_records_by_zero_ids() {
        final StandStorage storage = Given.newStorage();

        storage.readMultiple(Collections.<AggregateStateId>emptyList());
    }

    /*
     * Misc
     * ----
     */

    @Test
    public void be_auto_closable() throws Exception {
        try (StandStorage storage = Given.newStorage()) {
            assertTrue(storage.isOpen());
            assertFalse(storage.isClosed());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void fail_to_write_data_after_closed() throws Exception {
        final StandStorage storage = Given.newStorage();

        assertTrue(storage.isOpen());
        storage.close();
        assertTrue(storage.isClosed());

        writeToStorage(new Given.TestAggregate("42"), storage, Project.class);
    }

    @Test(expected = IllegalStateException.class)
    public void fail_to_read_data_after_closed() throws Exception {
        final StandStorage storage = Given.newStorage();

        assertTrue(storage.isOpen());
        storage.close();
        assertTrue(storage.isClosed());

        storage.readAll();
    }

    private static void assertMatches(Message message, FieldMask fieldMask) {
        final List<String> paths = fieldMask.getPathsList();
        for (Descriptors.FieldDescriptor field : message.getDescriptorForType()
                                                        .getFields()) {

            // Protobuf limitation, has no effect on the test.
            if (field.isRepeated()) {
                continue;
            }

            assertEquals(message.hasField(field), paths.contains(field.getFullName()));
        }
    }

    private static EntityRecord writeToStorage(Aggregate<?, ?, ?> aggregate,
                                                      StandStorage storage,
                                                      Class<? extends Message> stateClass) {
        final AggregateStateId id = AggregateStateId.of(aggregate.getId(), TypeUrl.of(stateClass));
        final EntityRecord record = EntityRecord.newBuilder()
                                                              .setState(AnyPacker.pack(
                                                                      aggregate.getState()))
                                                              .setVersion(org.spine3.base.Version.newBuilder().setNumber(1).setTimestamp(
                                                                      Timestamps2.getCurrentTime()))
                                                              .build();

        storage.write(id, record);

        return record;
    }

    private static class StandStorageRecord extends AbstractEntity<Object, Project> {

        protected StandStorageRecord(AggregateStateId id) {
            super(id);
        }
    }

    private static class Given {

        private static StandStorage newStorage() {
            final DataSourceWrapper dataSource = GivenDataSource.whichIsStoredInMemory(
                    GivenDataSource.DEFAULT_TABLE_NAME);

            final RecordStorageQueryFactory<String> queryFactory = new RecordStorageQueryFactory<>(
                    dataSource,
                    TestAggregate.class);

            final StandStorage storage = JdbcStandStorage.<String>newBuilder()
                    .setRecordStorageQueryFactory(queryFactory)
                    .setDataSource(dataSource)
                    .build();

            return storage;
        }

        private static class TestAggregate extends Aggregate<String, Project, Project.Builder> {

            /**
             * Creates a new aggregate instance.
             *
             * @param id the ID for the new aggregate
             * @throws IllegalArgumentException if the ID is not of one of the supported types
             */
            private TestAggregate(String id) {
                super(id);
            }

            private void setState(Project state) {
                incrementState(state);
            }
        }

        private static class TestAggregate2 extends Aggregate<String, Customer, Customer.Builder> {

            /**
             * Creates a new aggregate instance.
             *
             * @param id the ID for the new aggregate
             * @throws IllegalArgumentException if the ID is not of one of the supported types
             */
            private TestAggregate2(String id) {
                super(id);
            }

            private void setState(Customer state) {
                incrementState(state);
            }
        }

        private static List<TestAggregate> testAggregates(int amount) {
            final List<TestAggregate> aggregates = new LinkedList<>();

            for (int i = 0; i < amount; i++) {
                aggregates.add(new TestAggregate(String.valueOf(i)));
            }

            return aggregates;
        }

        private static List<TestAggregate> testAggregatesWithState(int amount) {
            final List<TestAggregate> aggregates = new LinkedList<>();

            for (int i = 0; i < amount; i++) {
                final TestAggregate aggregate = new TestAggregate(String.valueOf(i));
                final Project state = Project.newBuilder()
                                             .setId(ProjectId.newBuilder()
                                                             .setId(aggregate.getId()))
                                             .setName("Some project")
                                             .setStatus(Project.Status.CREATED)
                                             .build();

                aggregate.setState(state);

                aggregates.add(aggregate);
            }

            return aggregates;
        }
    }
}
