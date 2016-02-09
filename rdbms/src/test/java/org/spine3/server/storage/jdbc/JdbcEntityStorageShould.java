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

package org.spine3.server.storage.jdbc;

import com.google.protobuf.StringValue;
import com.google.protobuf.util.TimeUtil;
import org.junit.Test;
import org.spine3.server.Entity;
import org.spine3.server.storage.EntityStorage;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.EntityStorageShould;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.test.project.Project;
import org.spine3.test.project.ProjectId;

import static org.junit.Assert.*;
import static org.spine3.base.Identifiers.newUuid;
import static org.spine3.protobuf.Messages.toAny;
import static org.spine3.server.storage.jdbc.JdbcStorageFactoryShould.newInMemoryDataSource;
import static org.spine3.testdata.TestAggregateIdFactory.createProjectId;

/**
 * @author Alexander Litus
 */
@SuppressWarnings("InstanceMethodNamingConvention")
public class JdbcEntityStorageShould extends EntityStorageShould {

    @Override
    protected EntityStorage<String> getStorage() {
        final JdbcEntityStorage<String> storage = newStorage(TestEntityWithIdString.class);
        return storage;
    }

    private static <I> JdbcEntityStorage<I> newStorage(Class<? extends Entity<I, ?>> entityClass) {
        final DataSourceWrapper dataSource = newInMemoryDataSource("entityStorageTests");
        return JdbcEntityStorage.newInstance(dataSource, entityClass);
    }

    @Test
    public void write_and_read_record_by_Message_id() {
        final JdbcEntityStorage<ProjectId> storage = newStorage(TestEntityWithIdMessage.class);
        final ProjectId id = createProjectId(newUuid());
        testWriteAndReadRecord(id, storage);
    }

    @Test
    public void write_and_read_record_by_Long_id() {
        final JdbcEntityStorage<Long> storage = newStorage(TestEntityWithIdLong.class);
        final long id = 10L;
        testWriteAndReadRecord(id, storage);
    }

    @Test
    public void write_and_read_record_by_Integer_id() {
        final JdbcEntityStorage<Integer> storage = newStorage(TestEntityWithIdInteger.class);
        final int id = 10;
        testWriteAndReadRecord(id, storage);
    }

    private static <I> void testWriteAndReadRecord(I id, JdbcEntityStorage<I> storage) {
        final EntityStorageRecord expectedRecord = newEntityRecord();

        storage.write(id, expectedRecord);
        final EntityStorageRecord actualRecord = storage.read(id);

        assertEquals(expectedRecord, actualRecord);
    }

    @Test
    public void close_itself() {
        final JdbcEntityStorage<String> storage = newStorage(TestEntityWithIdString.class);
        storage.close();
        try {
            storage.readInternal("any-id");
        } catch (DatabaseException ignored) {
            // is OK because the storage is closed
            return;
        }
        fail("Storage must close itself.");
    }

    @Test
    public void clear_itself() {
        final JdbcEntityStorage<String> storage = newStorage(TestEntityWithIdString.class);
        final String id = newUuid();
        final EntityStorageRecord record = newEntityRecord();
        storage.writeInternal(id, record);
        storage.clear();

        final EntityStorageRecord actual = storage.readInternal(id);
        assertNull(actual);
    }

    private static EntityStorageRecord newEntityRecord() {
        final StringValue stringValue = StringValue.newBuilder().setValue(newUuid()).build();
        final EntityStorageRecord.Builder builder = EntityStorageRecord.newBuilder()
                .setState(toAny(stringValue))
                .setVersion(5) // set any non-default value
                .setWhenModified(TimeUtil.getCurrentTime());
        return builder.build();
    }

    private static class TestEntityWithIdMessage extends Entity<ProjectId, Project> {
        private TestEntityWithIdMessage(ProjectId id) {
            super(id);
        }
    }

    private static class TestEntityWithIdString extends Entity<String, Project> {
        private TestEntityWithIdString(String id) {
            super(id);
        }
    }

    private static class TestEntityWithIdInteger extends Entity<Integer, Project> {
        private TestEntityWithIdInteger(Integer id) {
            super(id);
        }
    }

    private static class TestEntityWithIdLong extends Entity<Long, Project> {
        private TestEntityWithIdLong(Long id) {
            super(id);
        }
    }
}
