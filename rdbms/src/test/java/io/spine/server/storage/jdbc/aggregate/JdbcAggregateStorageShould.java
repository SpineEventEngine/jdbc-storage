/*
 * Copyright 2017, TeamDev. All rights reserved.
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
import io.spine.server.storage.jdbc.query.DbIterator;
import io.spine.test.aggregate.ProjectId;
import org.junit.Test;

import java.sql.SQLException;

import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.PredefinedMapping.MYSQL_5_7;
import static io.spine.test.Tests.nullRef;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Alexander Litus
 */
public class JdbcAggregateStorageShould extends AggregateStorageShould {

    private JdbcAggregateStorage<ProjectId> storage;

    @Override
    public void setUpAbstractStorageTest() {
        super.setUpAbstractStorageTest();
        storage = (JdbcAggregateStorage<ProjectId>) getStorage();
    }

    @Test(expected = IllegalStateException.class)
    public void throw_exception_when_closing_twice() throws Exception {
        final AggregateStorage<?> storage = getStorage();
        storage.close();
        storage.close();
    }

    @Test
    public void return_history_iterator_with_specified_batch_size() throws SQLException {
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
        final Class<? extends Aggregate<Object, ?, ?>> nullClass = nullRef();
        JdbcAggregateStorage.newBuilder()
                            .setAggregateClass(nullClass);
    }

    @Override
    protected AggregateStorage<ProjectId> newStorage(Class<? extends Entity> aClass) {
        return newStorage(ProjectId.class, (Class<? extends Aggregate<ProjectId, ?, ?>>) aClass);
    }

    @Override
    protected <I> JdbcAggregateStorage<I> newStorage(
            Class<? extends I> idClass,
            Class<? extends Aggregate<I, ?, ?>> aggregateClass) {
        final DataSourceWrapper dataSource = whichIsStoredInMemory("aggregateStorageTests");
        final JdbcAggregateStorage.Builder<I> builder = JdbcAggregateStorage.newBuilder();
        final JdbcAggregateStorage<I> storage = builder.setMultitenant(false)
                                                       .setDataSource(dataSource)
                                                       .setAggregateClass(aggregateClass)
                                                       .setTypeMapping(MYSQL_5_7)
                                                       .build();
        return storage;
    }
}
