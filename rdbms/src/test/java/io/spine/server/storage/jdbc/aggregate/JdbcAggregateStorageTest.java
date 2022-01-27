/*
 * Copyright 2021, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import io.spine.environment.Tests;
import io.spine.server.ServerEnvironment;
import io.spine.server.aggregate.AggregateStorageTest;
import io.spine.server.storage.jdbc.JdbcStorageFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;

import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.PredefinedMapping.H2_2_1;

//@SuppressWarnings("DuplicateStringLiteralInspection") // Common test display names.
@DisplayName("RDBMS-backed `AggregateStorage` should")
class JdbcAggregateStorageTest extends AggregateStorageTest {

    private JdbcStorageFactory factory;

    //TODO:2021-07-01:alex.tymchenko: do we need to `setUp` and `tearDown`?
    @BeforeEach
    void setUp() {
        factory = JdbcStorageFactory.newBuilder()
                .setDataSource(whichIsStoredInMemory(newUuid()))
                .setTypeMapping(H2_2_1)
                .build();
        ServerEnvironment
                .when(Tests.class)
                .use(factory);
    }

    @AfterEach
    void tearDown() {
        ServerEnvironment.instance().reset();
        if(factory != null) {
            factory.close();
        }
    }

//    private JdbcAggregateStorage<ProjectId> storage;
//
//    @BeforeEach
//    @Override
//    public void setUpAbstractStorageTest() {
//        super.setUpAbstractStorageTest();
//        storage = (JdbcAggregateStorage<ProjectId>) storage();
//    }
//
//    @Test
//    @DisplayName("throw ISE when closing twice")
//    void throwOnClosingTwice() {
//        AggregateStorage<?> storage = storage();
//        storage.close();
//        assertThrows(IllegalStateException.class, storage::close);
//    }
//
//    @Test
//    @DisplayName("return history iterator with specified batch size")
//    void returnHistoryIterator() throws SQLException {
//        int batchSize = 10;
//        AggregateReadRequest<ProjectId> request = new AggregateReadRequest<>(newId(), batchSize);
//        DbIterator<AggregateEventRecord> iterator =
//                (DbIterator<AggregateEventRecord>) storage.historyBackward(request);
//
//        // Use `PreparedStatement.getFetchSize()` instead of `ResultSet.getFetchSize()`,
//        // because the result of the latter depends on a JDBC driver implementation.
//        int fetchSize = iterator.resultSet()
//                                .getStatement()
//                                .getFetchSize();
//        assertEquals(batchSize, fetchSize);
//    }
//
//    @Test
//    @DisplayName("close history iterator")
//    void closeHistoryIterator() throws SQLException {
//        AggregateReadRequest<ProjectId> request = newReadRequest(newId());
//        DbIterator<AggregateEventRecord> iterator =
//                (DbIterator<AggregateEventRecord>) storage.historyBackward(request);
//
//        storage.close();
//        boolean historyIteratorClosed = iterator.resultSet()
//                                                .isClosed();
//        assertTrue(historyIteratorClosed);
//    }
//
//    @Test
//    @DisplayName("require non-null aggregate class")
//    void rejectNullAggregateClass() {
//        Class<? extends Aggregate<Object, ?, ?>> nullClass = nullRef();
//        assertThrows(NullPointerException.class,
//                     () -> JdbcAggregateStorage.newBuilder()
//                                               .setAggregateClass(nullClass));
//    }
//
//    @SuppressWarnings("unchecked") // It is OK for a test.
//    @Override
//    protected AggregateStorage<ProjectId> newStorage(Class<? extends Entity<?, ?>> aClass) {
//        return newStorage(ProjectId.class, (Class<? extends Aggregate<ProjectId, ?, ?>>) aClass);
//    }
//
//    @Override
//    protected <I> JdbcAggregateStorage<I> newStorage(
//            Class<? extends I> idClass,
//            Class<? extends Aggregate<I, ?, ?>> aggregateClass) {
//        DataSourceWrapper dataSource = whichIsStoredInMemory("aggregateStorageTests");
//        JdbcAggregateStorage.Builder<I> builder = JdbcAggregateStorage.newBuilder();
//        JdbcAggregateStorage<I> storage = builder.setMultitenant(false)
//                                                 .setDataSource(dataSource)
//                                                 .setAggregateClass(aggregateClass)
//                                                 .setTypeMapping(H2_1_4)
//                                                 .build();
//        return storage;
//    }
}
