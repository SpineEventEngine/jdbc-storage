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

import io.spine.server.aggregate.Aggregate;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.aggregate.AggregateReadRequest;
import io.spine.server.aggregate.AggregateStorage;
import io.spine.server.aggregate.AggregateStorageShould;
import io.spine.server.entity.Entity;
import io.spine.test.aggregate.Project;
import io.spine.test.aggregate.ProjectId;
import io.spine.validate.ValidatingBuilder;
import org.junit.Test;

import java.sql.SQLException;

import static io.spine.test.Tests.nullRef;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Alexander Litus
 */
public class JdbcAggregateStorageShould extends AggregateStorageShould {

    @Override
    protected AggregateStorage<ProjectId> getStorage(Class<? extends Entity> aClass) {
        return nullRef();
    }

    @Override
    protected <I> JdbcAggregateStorage<I> getStorage(
            Class<? extends I> idClass,
            Class<? extends Aggregate<I, ?, ?>> aggregateClass) {
        final DataSourceWrapper dataSource =
                GivenDataSource.whichIsStoredInMemory("aggregateStorageTests");
        final JdbcAggregateStorage<I> storage =
                JdbcAggregateStorage.<I>newBuilder()
                        .setMultitenant(false)
                        .setDataSource(dataSource)
                        .setAggregateClass(aggregateClass)
                        .build();
        return storage;
    }

    @Test
    public void throw_exception_if_try_to_use_closed_storage() {
        final JdbcAggregateStorage<ProjectId> storage = getStorage(ProjectId.class,
                                                                   TestAggregateWithMessageId.class);
        storage.close();
        try {
            final AggregateReadRequest<ProjectId> request = newReadRequest(newId());
            storage.historyBackward(request);
        } catch (DatabaseException expected) {
            // expected exception because the storage is closed
            return;
        }
        fail("Aggregate storage should close itself.");
    }

    @Test(expected = IllegalStateException.class)
    public void throw_exception_when_closing_twice() throws Exception {
        final AggregateStorage<?> storage = getStorage(ProjectId.class,
                                                       TestAggregateWithMessageId.class);
        storage.close();
        storage.close();
    }

    @Test
    public void return_history_iterator_with_specified_batch_size() throws SQLException {
        final JdbcAggregateStorage<ProjectId> storage = getStorage(ProjectId.class,
                                                                   TestAggregateWithMessageId.class);
        final int batchSize = 10;
        final AggregateReadRequest<ProjectId> request = new AggregateReadRequest<>(newId(),
                                                                                   batchSize);
        final DbIterator<AggregateEventRecord> iterator =
                (DbIterator<AggregateEventRecord>) storage.historyBackward(request);

        // Use `PreparedStatement.getFetchSize()` instead of `ResultSet.getFetchSize()`,
        // because the result of the latter depends on a JDBC driver implementation.
        final int fetchSize = iterator.getResultSet()
                                      .getStatement()
                                      .getFetchSize();
        assertEquals(batchSize, fetchSize);
    }

    @Test
    public void close_history_iterator() throws SQLException {
        final JdbcAggregateStorage<ProjectId> storage = getStorage(ProjectId.class,
                                                                   TestAggregateWithMessageId.class);
        final AggregateReadRequest<ProjectId> request = newReadRequest(newId());
        final DbIterator<AggregateEventRecord> iterator =
                (DbIterator<AggregateEventRecord>) storage.historyBackward(request);

        storage.close();
        final boolean historyIteratorClosed = iterator.getResultSet()
                                                      .isClosed();
        assertTrue(historyIteratorClosed);
    }

    private static class TestAggregateWithMessageId extends Aggregate<ProjectId,
            Project,
            ValidatingBuilder<Project, Project.Builder>> {
        private TestAggregateWithMessageId(ProjectId id) {
            super(id);
        }
    }
}
