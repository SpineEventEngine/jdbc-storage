/*
 * Copyright 2022, TeamDev. All rights reserved.
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

import io.spine.server.aggregate.Aggregate;
import io.spine.server.aggregate.AggregateStorage;
import io.spine.server.aggregate.AggregateStorageTest;
import io.spine.server.entity.Entity;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.test.aggregate.ProjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.PredefinedMapping.H2_2_1;
import static io.spine.testing.Tests.nullRef;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings("DuplicateStringLiteralInspection") // Common test display names.
@DisplayName("JdbcAggregateStorage should")
public class JdbcAggregateStorageTest extends AggregateStorageTest {

    @Test
    @DisplayName("throw ISE when closing twice")
    void throwOnClosingTwice() {
        AggregateStorage<?> storage = storage();
        storage.close();
        assertThrows(IllegalStateException.class, storage::close);
    }

    @Test
    @DisplayName("require non-null aggregate class")
    void rejectNullAggregateClass() {
        Class<? extends Aggregate<Object, ?, ?>> nullClass = nullRef();
        assertThrows(NullPointerException.class,
                     () -> JdbcAggregateStorage.newBuilder()
                                               .setAggregateClass(nullClass));
    }

    @SuppressWarnings("unchecked") // It is OK for a test.
    @Override
    protected AggregateStorage<ProjectId> newStorage(Class<? extends Entity<?, ?>> aClass) {
        return newStorage(ProjectId.class, (Class<? extends Aggregate<ProjectId, ?, ?>>) aClass);
    }

    @Override
    protected <I> JdbcAggregateStorage<I> newStorage(
            Class<? extends I> idClass,
            Class<? extends Aggregate<I, ?, ?>> aggregateClass) {
        DataSourceWrapper dataSource = whichIsStoredInMemory("aggregateStorageTests");
        JdbcAggregateStorage.Builder<I> builder = JdbcAggregateStorage.newBuilder();
        JdbcAggregateStorage<I> storage = builder.setMultitenant(false)
                                                 .setDataSource(dataSource)
                                                 .setAggregateClass(aggregateClass)
                                                 .setTypeMapping(H2_2_1)
                                                 .build();
        return storage;
    }
}
