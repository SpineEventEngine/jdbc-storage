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

import com.zaxxer.hikari.HikariConfig;
import org.junit.After;
import org.junit.Test;
import org.spine3.base.Event;
import org.spine3.server.aggregate.Aggregate;
import org.spine3.server.storage.AggregateEvents;
import org.spine3.server.storage.AggregateStorage;
import org.spine3.server.storage.AggregateStorageShould;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.HikariDataSourceWrapper;
import org.spine3.test.project.Project;
import org.spine3.test.project.ProjectId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.spine3.server.Identifiers.newUuid;
import static org.spine3.server.storage.jdbc.JdbcStorageFactoryShould.getInMemoryDbUrl;
import static org.spine3.testdata.TestAggregateIdFactory.createProjectId;
import static org.spine3.testdata.TestEventFactory.projectCreated;

/**
 * @author Alexander Litus
 */
@SuppressWarnings("InstanceMethodNamingConvention")
public class JdbcAggregateStorageShould extends AggregateStorageShould {

    /**
     * The URL of an in-memory DB.
     */
    private static final String DB_URL = getInMemoryDbUrl("aggregateStorageTests");

    private final JdbcAggregateStorage<ProjectId> storage = newStorage(TestAggregateWithIdMessage.class);

    @Override
    protected AggregateStorage<ProjectId> getStorage() {
        return storage;
    }

    private static <I> JdbcAggregateStorage<I> newStorage(Class<? extends Aggregate<I, ?>> aggregateClass) {
        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl(DB_URL);
        // not setting username and password is OK for in-memory database
        final DataSourceWrapper dataSource = HikariDataSourceWrapper.newInstance(config);
        return JdbcAggregateStorage.newInstance(dataSource, aggregateClass);
    }

    @After
    public void tearDownTest() {
        storage.close();
    }

    @Test
    public void close_itself() {
        final JdbcAggregateStorage<ProjectId> storage = newStorage(TestAggregateWithIdMessage.class);
        storage.close();
        try {
            storage.historyBackward(createProjectId("anyId"));
        } catch (DatabaseException ignored) {
            // is OK because the storage is closed
            return;
        }
        fail("Aggregate storage should close itself.");
    }

    @Test
    public void write_and_read_event_by_String_id() {
        final JdbcAggregateStorage<String> storage = newStorage(TestAggregateWithIdString.class);
        final String id = newUuid();
        testWriteAndReadEvent(id, storage);
    }

    @Test
    public void write_and_read_event_by_Long_id() {
        final JdbcAggregateStorage<Long> storage = newStorage(TestAggregateWithIdLong.class);
        final long id = 10L;
        testWriteAndReadEvent(id, storage);
    }

    @Test
    public void write_and_read_event_by_Integer_id() {
        final JdbcAggregateStorage<Integer> storage = newStorage(TestAggregateWithIdInteger.class);
        final int id = 10;
        testWriteAndReadEvent(id, storage);
    }

    private static <I> void testWriteAndReadEvent(I id, JdbcAggregateStorage<I> storage) {
        final Event expectedEvent = projectCreated();

        storage.writeEvent(id, expectedEvent);

        final AggregateEvents events = storage.read(id);
        assertEquals(1, events.getEventCount());
        final Event actualEvent = events.getEvent(0);

        assertEquals(expectedEvent, actualEvent);
    }

    private static class TestAggregateWithIdMessage extends Aggregate<ProjectId, Project> {
        private TestAggregateWithIdMessage(ProjectId id) {
            super(id);
        }
    }

    private static class TestAggregateWithIdString extends Aggregate<String, Project> {
        private TestAggregateWithIdString(String id) {
            super(id);
        }
    }

    private static class TestAggregateWithIdInteger extends Aggregate<Integer, Project> {
        private TestAggregateWithIdInteger(Integer id) {
            super(id);
        }
    }

    private static class TestAggregateWithIdLong extends Aggregate<Long, Project> {
        private TestAggregateWithIdLong(Long id) {
            super(id);
        }
    }
}
