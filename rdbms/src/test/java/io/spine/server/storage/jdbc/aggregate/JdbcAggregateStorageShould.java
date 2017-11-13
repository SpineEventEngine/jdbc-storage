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

package io.spine.server.storage.jdbc.aggregate;

import io.spine.server.aggregate.Aggregate;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.aggregate.AggregateReadRequest;
import io.spine.server.aggregate.AggregateStorage;
import io.spine.server.aggregate.AggregateStorageShould;
import io.spine.server.entity.Entity;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.GivenDataSource;
import io.spine.server.storage.jdbc.query.DbIterator;
import io.spine.test.Tests;
import io.spine.test.aggregate.Project;
import io.spine.test.aggregate.ProjectId;
import io.spine.test.aggregate.ProjectVBuilder;
import org.junit.Test;

import java.sql.SQLException;

import static io.spine.test.Tests.nullRef;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

    @Test(expected = IllegalStateException.class)
    public void throw_exception_when_closing_twice() throws Exception {
        final AggregateStorage<?> storage = getStorage(ProjectId.class,
                                                       TestAggregateWithMessageId.class);
        storage.close();
        storage.close();
    }

    @SuppressWarnings("unchecked") // OK for a mock.
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

    @Test(expected = NullPointerException.class)
    public void require_non_null_aggregate_class() {
        final Class<? extends Aggregate<Object, ?, ?>> nullClass = Tests.nullRef();
        JdbcAggregateStorage.newBuilder()
                            .setAggregateClass(nullClass);
    }

    private static class TestAggregateWithMessageId extends Aggregate<ProjectId,
                                                                      Project,
                                                                      ProjectVBuilder> {
        private TestAggregateWithMessageId(ProjectId id) {
            super(id);
        }
    }
}
