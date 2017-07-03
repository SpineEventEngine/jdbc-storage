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

package io.spine.server.storage.jdbc.stand;

import com.google.common.base.Optional;
import com.google.protobuf.Descriptors;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import io.spine.core.Version;
import io.spine.protobuf.AnyPacker;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.entity.Entity;
import io.spine.server.entity.EntityRecord;
import io.spine.server.stand.AggregateStateId;
import io.spine.server.stand.StandStorage;
import io.spine.server.stand.StandStorageShould;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.GivenDataSource;
import io.spine.server.storage.jdbc.JdbcStandStorage;
import io.spine.server.storage.jdbc.util.ConnectionWrapper;
import io.spine.server.storage.jdbc.util.DataSourceWrapper;
import io.spine.test.commandservice.customer.Customer;
import io.spine.test.storage.Project;
import io.spine.test.storage.ProjectId;
import io.spine.time.Time;
import io.spine.type.TypeUrl;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static io.spine.server.storage.jdbc.stand.Given.TestAggregate;
import static io.spine.server.storage.jdbc.stand.Given.TestAggregate2;
import static io.spine.server.storage.jdbc.stand.Given.newStorage;
import static io.spine.server.storage.jdbc.stand.Given.testAggregates;
import static io.spine.server.storage.jdbc.stand.Given.testAggregatesWithState;
import static io.spine.test.Verify.assertContains;
import static io.spine.test.Verify.assertSize;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Dmytro Dashenkov
 */
public class JdbcStandStorageShould extends StandStorageShould {

    @Override
    protected StandStorage getStorage(Class<? extends Entity> aClass) {
        final DataSourceWrapper dataSource = GivenDataSource.whichIsStoredInMemory(
                "StandStorageTests");
        final StandStorage storage = JdbcStandStorage.newBuilder()
                                                     .setDataSource(dataSource)
//                                                     .setEntityClass(aClass)
                                                     .setMultitenant(false)
                                                     .build();
        return storage;
    }

    /*
     * Initialize tests
     * ----------------
     */

    @SuppressWarnings("unchecked") // For mocks
    @Test
    public void initialize_properly_with_all_builder_fields() {
        final DataSourceWrapper dataSourceMock = mock(DataSourceWrapper.class);
        final ConnectionWrapper connectionMock = mock(ConnectionWrapper.class);
        final PreparedStatement statementMock = mock(PreparedStatement.class);

        when(connectionMock.prepareStatement(anyString())).thenReturn(statementMock);
        when(dataSourceMock.getConnection(anyBoolean())).thenReturn(connectionMock);

        final StandStorage standStorage = JdbcStandStorage.<String>newBuilder()
                .setDataSource(dataSourceMock)
                .setMultitenant(false)
//                .setEntityClass(TestAggregate.class)
                .build();

        assertNotNull(standStorage);
        // Established connection with the DB
        verify(dataSourceMock).getConnection(anyBoolean());
    }

    @SuppressWarnings("unchecked") // For mocks
    @Test
    public void initialize_properly_without_multitenancy() {
        final DataSourceWrapper dataSourceMock = mock(DataSourceWrapper.class);
        final ConnectionWrapper connectionMock = mock(ConnectionWrapper.class);
        final PreparedStatement statementMock = mock(PreparedStatement.class);

        when(connectionMock.prepareStatement(anyString())).thenReturn(statementMock);
        when(dataSourceMock.getConnection(anyBoolean())).thenReturn(connectionMock);

        final StandStorage standStorage = JdbcStandStorage.<String>newBuilder()
                .setDataSource(dataSourceMock)
//                .setEntityClass(TestAggregate.class)
                .build();

        assertNotNull(standStorage);
        assertFalse(standStorage.isMultitenant());
    }

    @Test(expected = IllegalStateException.class)
    public void fail_to_initialize_with_empty_builder() {
        JdbcStandStorage.newBuilder()
                        .build();
    }

    @Test(expected = IllegalStateException.class)
    public void fail_to_initialize_without_data_source() {
        JdbcStandStorage.newBuilder()
                        .setMultitenant(false)
//                        .setEntityClass(StandStorageRecord.class)
                        .build();
    }

    /*
     * Read-write positive tests
     * -------------------------
     */

    @Test
    public void write_data_to_store() {
        final StandStorage storage = newStorage();

        final TestAggregate aggregate = new TestAggregate("some_id");

        final EntityRecord record = writeToStorage(aggregate, storage, Project.class);

        final Optional<EntityRecord> readRecord = storage.read(
                AggregateStateId.of(aggregate.getId(),
                                    TypeUrl.of(Project.class)));
        assertTrue(readRecord.isPresent());
        @SuppressWarnings("OptionalGetWithoutIsPresent") // We do check if present
        final EntityRecord actualRecord = readRecord.get();
        assertEquals(actualRecord, record);
    }

    @Test
    public void perform_bulk_read_operations() {
        final StandStorage storage = newStorage();

        final Collection<Given.TestAggregate> testData = testAggregates(10);

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

        final Collection<EntityRecord> readRecords =
                (Collection<EntityRecord>) storage.readMultiple(ids);
        assertEquals(ids.size(), readRecords.size());

        assertContains(records.get(1), readRecords);
        assertContains(records.get(2), readRecords);
        assertContains(records.get(3), readRecords);
        assertContains(records.get(5), readRecords);
        assertContains(records.get(8), readRecords);
    }

    @Test
    public void handle_wrong_ids_silently() {
        final StandStorage storage = newStorage();

        final TypeUrl typeUrl = TypeUrl.of(Project.class);
        final String repeatingInvalidId = "invalid-id-1";

        final Collection<AggregateStateId> ids = new LinkedList<>();
        ids.add(AggregateStateId.of(repeatingInvalidId, typeUrl));
        ids.add(AggregateStateId.of("invalid-id-2", typeUrl));
        ids.add(AggregateStateId.of(repeatingInvalidId, typeUrl));

        final Collection<EntityRecord> records = (Collection<EntityRecord>)
                storage.readMultiple(ids);

        assertNotNull(records);
        assertSize(0, records);
    }

    @SuppressWarnings("MethodWithMultipleLoops")
    @Test
    public void read_all_from_database() {
        final StandStorage storage = newStorage();

        final Collection<Given.TestAggregate> testData = testAggregates(10);

        final List<EntityRecord> records = new ArrayList<>();

        for (Aggregate aggregate : testData) {
            records.add(writeToStorage(aggregate, storage, Project.class));
        }
        final Iterator<EntityRecord> readRecords = storage.readAll();
        int iteratorCounter = 0;
        while(readRecords.hasNext()) {
            assertTrue(records.contains(readRecords.next()));
            iteratorCounter++;
        }

        assertEquals(records.size(), iteratorCounter);
    }

    @Test
    public void apply_field_mask_to_read_values() {
        final StandStorage storage = newStorage();

        final String stringId = "42";
        final AggregateStateId id = AggregateStateId.of(stringId, TypeUrl.of(Project.class));

        final Project project = Project.newBuilder()
                                       .setId(ProjectId.newBuilder()
                                                       .setId(stringId))
                                       .setName("Some name")
                                       .setStatus(Project.Status.DONE)
                                       .build();

        final TestAggregate aggregate = new TestAggregate(stringId);
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
        @SuppressWarnings("OptionalGetWithoutIsPresent") // We do check is present
        final EntityRecord record = recordOptional.get();
        final Project withIdOnly = AnyPacker.unpack(record.getState());
        final Project withIdAndName = AnyPacker.unpack(
                storage.readMultiple(Collections.singleton(id), idAndName)
                       .next()
                       .getState());
        final Project withNameAndStatus = AnyPacker.unpack(storage.readAll(nameAndStatus)
                                                                  .next()
                                                                  .getState());

        assertMatches(withIdOnly, idOnly);
        assertMatches(withIdAndName, idAndName);
        assertMatches(withNameAndStatus, nameAndStatus);
    }

    @Test
    public void read_all_by_type_url() {
        final StandStorage storage = newStorage();

        final int aggregatesCount = 5;
        final List<Given.TestAggregate> aggregates = testAggregates(aggregatesCount);
        final TestAggregate2 differentAggregate = new TestAggregate2("i_am_different");

        for (Aggregate aggregate : aggregates) {
            writeToStorage(aggregate, storage, Project.class);
        }

        final EntityRecord differentRecord =
                writeToStorage(differentAggregate, storage, Customer.class);

        final Iterator<EntityRecord> records = storage.readAllByType(TypeUrl.of(Project.class));
        final Collection<EntityRecord> readRecords = newArrayList(records);
        assertSize(aggregatesCount, readRecords);
        boolean hasRecord = false;
        for (EntityRecord record : readRecords) {
            if (record.equals(differentRecord)) {
                hasRecord = true;
            }
        }
        assertFalse(hasRecord);
    }

    @SuppressWarnings("MethodWithMultipleLoops")
    @Test
    public void read_by_type_and_apply_field_mask() {
        final StandStorage storage = newStorage();

        final List<Given.TestAggregate> aggregates = testAggregatesWithState(5);

        for (Aggregate aggregate : aggregates) {
            writeToStorage(aggregate, storage, Project.class);
        }

        final FieldMask namesMask = FieldMask.newBuilder()
                                             .addPaths(Project.getDescriptor()
                                                              .getFields()
                                                              .get(1)
                                                              .getFullName())
                                             .build();

        final Iterator<EntityRecord> records = storage.readAllByType(
                TypeUrl.of(Project.class), namesMask);

        while (records.hasNext()) {
            final Project project = AnyPacker.unpack(records.next()
                                                            .getState());
            assertMatches(project, namesMask);
        }
    }

    /*
     * Read-write negative tests
     * -------------------------
     */

    @Test(expected = DatabaseException.class)
    public void fail_to_fetch_records_by_zero_ids() {
        final StandStorage storage = newStorage();

        storage.readMultiple(Collections.<AggregateStateId>emptyList());
    }

    /*
     * Misc
     * ----
     */

    @Test
    public void be_auto_closable() throws Exception {
        try (StandStorage storage = newStorage()) {
            assertTrue(storage.isOpen());
            assertFalse(storage.isClosed());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void fail_to_write_data_after_closed() throws Exception {
        final StandStorage storage = newStorage();

        assertTrue(storage.isOpen());
        storage.close();
        assertTrue(storage.isClosed());

        writeToStorage(new TestAggregate("42"), storage, Project.class);
    }

    @Test(expected = IllegalStateException.class)
    public void fail_to_read_data_after_closed() throws Exception {
        final StandStorage storage = newStorage();

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
        final Version version = Version.newBuilder()
                                       .setNumber(1)
                                       .setTimestamp(Time.getCurrentTime())
                                       .build();
        final EntityRecord record =
                EntityRecord.newBuilder()
                            .setState(AnyPacker.pack(aggregate.getState()))
                            .setVersion(version)
                            .build();

        storage.write(id, record);

        return record;
    }
}