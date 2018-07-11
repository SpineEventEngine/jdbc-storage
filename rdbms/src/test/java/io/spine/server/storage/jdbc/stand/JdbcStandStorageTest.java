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

package io.spine.server.storage.jdbc.stand;

import com.google.common.base.Optional;
import com.google.protobuf.Descriptors;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import io.spine.base.Time;
import io.spine.core.Version;
import io.spine.protobuf.AnyPacker;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.entity.Entity;
import io.spine.server.entity.EntityRecord;
import io.spine.server.stand.AggregateStateId;
import io.spine.server.stand.StandStorage;
import io.spine.server.stand.StandStorageTest;
import io.spine.server.storage.RecordReadRequest;
import io.spine.server.storage.jdbc.ConnectionWrapper;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.GivenDataSource;
import io.spine.server.storage.jdbc.stand.given.Given;
import io.spine.test.commandservice.customer.Customer;
import io.spine.test.storage.Project;
import io.spine.type.TypeUrl;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static io.spine.server.storage.jdbc.PredefinedMapping.MYSQL_5_7;
import static io.spine.server.storage.jdbc.stand.given.Given.TestAggregate;
import static io.spine.server.storage.jdbc.stand.given.Given.TestAggregate2;
import static io.spine.server.storage.jdbc.stand.given.Given.testAggregates;
import static io.spine.server.storage.jdbc.stand.given.Given.testAggregatesWithState;
import static io.spine.test.Verify.assertContains;
import static io.spine.test.Verify.assertSize;
import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Dmytro Dashenkov
 */
@DisplayName("JdbcStandStorage should")
class JdbcStandStorageTest extends StandStorageTest {

    @Override
    protected StandStorage newStorage(Class<? extends Entity> entityClass) {
        final DataSourceWrapper dataSource = GivenDataSource.whichIsStoredInMemory(
                "StandStorageTests");
        final StandStorage storage = JdbcStandStorage.newBuilder()
                                                     .setDataSource(dataSource)
                                                     .setMultitenant(false)
                                                     .setTypeMapping(MYSQL_5_7)
                                                     .build();
        return storage;
    }

    @SuppressWarnings("unchecked") // For mocks.
    @Test
    @DisplayName("initialize properly with all builder fields")
    void initializeProperlyWithAllBuilderFields() {
        final DataSourceWrapper dataSourceMock = mock(DataSourceWrapper.class);
        final ConnectionWrapper connectionMock = mock(ConnectionWrapper.class);
        final PreparedStatement statementMock = mock(PreparedStatement.class);

        when(connectionMock.prepareStatement(anyString())).thenReturn(statementMock);
        when(dataSourceMock.getConnection(anyBoolean())).thenReturn(connectionMock);

        final StandStorage standStorage = JdbcStandStorage.<String>newBuilder()
                                                          .setDataSource(dataSourceMock)
                                                          .setMultitenant(false)
                                                          .setTypeMapping(MYSQL_5_7)
                                                          .build();

        assertNotNull(standStorage);
        // Established connection with the DB
        verify(dataSourceMock).getConnection(anyBoolean());
    }

    @SuppressWarnings("unchecked") // For mocks.
    @Test
    @DisplayName("initialize properly without multitenancy")
    void initializeProperlyWithoutMultitenancy() {
        final DataSourceWrapper dataSourceMock = mock(DataSourceWrapper.class);
        final ConnectionWrapper connectionMock = mock(ConnectionWrapper.class);
        final PreparedStatement statementMock = mock(PreparedStatement.class);

        when(connectionMock.prepareStatement(anyString())).thenReturn(statementMock);
        when(dataSourceMock.getConnection(anyBoolean())).thenReturn(connectionMock);

        final StandStorage standStorage = JdbcStandStorage.<String>newBuilder()
                                                          .setDataSource(dataSourceMock)
                                                          .setTypeMapping(MYSQL_5_7)
                                                          .build();

        assertNotNull(standStorage);
        assertFalse(standStorage.isMultitenant());
    }

    @Test
    @DisplayName("fail to initialize with empty builder")
    void failToInitializeWithEmptyBuilder() {
        assertThrows(IllegalStateException.class, () -> JdbcStandStorage.newBuilder()
                                                                        .build());
    }

    @Test
    @DisplayName("fail to initialize without data source")
    void failToInitializeWithoutDataSource() {
        assertThrows(IllegalStateException.class, () -> JdbcStandStorage.newBuilder()
                                                                        .setMultitenant(false)
                                                                        .build());
    }

    /*
     * Read-write positive tests
     * -------------------------
     */

    @Test
    @DisplayName("write data to store")
    void writeDataToStore() {
        final StandStorage storage = getStorage();

        final TestAggregate aggregate = new TestAggregate("some_id");

        final EntityRecord record = writeToStorage(aggregate, storage, Project.class);

        final AggregateStateId id = AggregateStateId.of(aggregate.getId(),
                                                        TypeUrl.of(Project.class));
        final RecordReadRequest<AggregateStateId> request = new RecordReadRequest<>(id);
        final Optional<EntityRecord> readRecord = storage.read(request);
        assertTrue(readRecord.isPresent());
        @SuppressWarnings("OptionalGetWithoutIsPresent") // We do check if present
        final EntityRecord actualRecord = readRecord.get();
        assertEquals(actualRecord, record);
    }

    @Test
    @DisplayName("perform bulk read operations")
    void performBulkReadOperations() {
        final StandStorage storage = getStorage();

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

        final Collection<EntityRecord> readRecords = newArrayList(storage.readMultiple(ids));
        assertEquals(ids.size(), readRecords.size());

        assertContains(records.get(1), readRecords);
        assertContains(records.get(2), readRecords);
        assertContains(records.get(3), readRecords);
        assertContains(records.get(5), readRecords);
        assertContains(records.get(8), readRecords);
    }

    @Test
    @DisplayName("handle wrong IDs silently")
    void handleWrongIdsSilently() {
        final StandStorage storage = getStorage();

        final TypeUrl typeUrl = TypeUrl.of(Project.class);
        final String repeatingInvalidId = "invalid-id-1";

        final Collection<AggregateStateId> ids = new LinkedList<>();
        ids.add(AggregateStateId.of(repeatingInvalidId, typeUrl));
        ids.add(AggregateStateId.of("invalid-id-2", typeUrl));
        ids.add(AggregateStateId.of(repeatingInvalidId, typeUrl));

        final Collection<?> records = newArrayList(storage.readMultiple(ids));

        assertNotNull(records);
        assertThat(records, empty());
    }

    @SuppressWarnings("MethodWithMultipleLoops")
    @Test
    @DisplayName("read all from database")
    void readAllFromDatabase() {
        final StandStorage storage = getStorage();

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
    @DisplayName("read all by type url")
    void readAllByTypeUrl() {
        final StandStorage storage = getStorage();

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
    @DisplayName("read by type and apply field mask")
    void readByTypeAndMask() {
        final StandStorage storage = getStorage();

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
     * Misc
     * ----
     */

    @Test
    @DisplayName("be auto-closable")
    void beAutoClosable() throws Exception {
        try (StandStorage storage = getStorage()) {
            assertTrue(storage.isOpen());
            assertFalse(storage.isClosed());
        }
    }

    @Test
    @DisplayName("fail to write data after closed")
    void failToWriteAfterClosed() throws Exception {
        final StandStorage storage = getStorage();

        assertTrue(storage.isOpen());
        storage.close();
        assertTrue(storage.isClosed());

        assertThrows(IllegalStateException.class,
                     () -> writeToStorage(new TestAggregate("42"), storage, Project.class));
    }

    @Test
    @DisplayName("fail to read data after closed")
    void failToReadAfterClosed() throws Exception {
        final StandStorage storage = getStorage();

        assertTrue(storage.isOpen());
        storage.close();
        assertTrue(storage.isClosed());

        assertThrows(IllegalStateException.class, storage::readAll);
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
