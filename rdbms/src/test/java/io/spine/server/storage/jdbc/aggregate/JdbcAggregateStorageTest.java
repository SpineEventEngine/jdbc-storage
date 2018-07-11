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

package io.spine.server.storage.jdbc.aggregate;

import io.spine.server.aggregate.Aggregate;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.aggregate.AggregateReadRequest;
import io.spine.server.aggregate.AggregateStorage;
import io.spine.server.aggregate.AggregateStorageTest;
import io.spine.server.entity.Entity;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.query.DbIterator;
import io.spine.test.aggregate.ProjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.PredefinedMapping.MYSQL_5_7;
import static io.spine.test.Tests.nullRef;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Alexander Litus
 */
@SuppressWarnings("DuplicateStringLiteralInspection") // Common test display names.
@DisplayName("JdbcAggregateStorage should")
class JdbcAggregateStorageTest extends AggregateStorageTest {

    private JdbcAggregateStorage<ProjectId> storage;

    @BeforeEach
    @Override
    public void setUpAbstractStorageTest() {
        super.setUpAbstractStorageTest();
        storage = (JdbcAggregateStorage<ProjectId>) getStorage();
    }

    @Test
    @DisplayName("throw ISE when closing twice")
    void throwOnClosingTwice() throws Exception {
        final AggregateStorage<?> storage = getStorage();
        storage.close();
        assertThrows(IllegalStateException.class, storage::close);
    }

    @Test
    @DisplayName("return history iterator with specified batch size")
    void returnHistoryIterator() throws SQLException {
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
    @DisplayName("close history iterator")
    void closeHistoryIterator() throws SQLException {
        final AggregateReadRequest<ProjectId> request = newReadRequest(newId());
        final DbIterator<AggregateEventRecord> iterator =
                (DbIterator<AggregateEventRecord>) storage.historyBackward(request);

        storage.close();
        final boolean historyIteratorClosed = iterator.getResultSet()
                                                      .isClosed();
        assertTrue(historyIteratorClosed);
    }

    @Test
    @DisplayName("require non-null aggregate class")
    void rejectNullAggregateClass() {
        final Class<? extends Aggregate<Object, ?, ?>> nullClass = nullRef();
        assertThrows(NullPointerException.class,
                     () -> JdbcAggregateStorage.newBuilder()
                                               .setAggregateClass(nullClass));
    }

    @SuppressWarnings("unchecked") // It is OK for a test.
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