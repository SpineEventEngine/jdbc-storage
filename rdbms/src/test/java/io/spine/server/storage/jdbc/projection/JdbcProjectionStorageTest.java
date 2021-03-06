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

package io.spine.server.storage.jdbc.projection;

import io.spine.server.entity.Entity;
import io.spine.server.projection.Projection;
import io.spine.server.projection.ProjectionStorage;
import io.spine.server.projection.ProjectionStorageTest;
import io.spine.server.storage.given.RecordStorageTestEnv.TestCounterEntity;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.StorageBuilder;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.record.JdbcRecordStorage;
import io.spine.test.storage.ProjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.PredefinedMapping.H2_1_4;
import static io.spine.testing.Tests.nullRef;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings("DuplicateStringLiteralInspection") // Common test display names.
@DisplayName("JdbcProjectionStorage should")
class JdbcProjectionStorageTest extends ProjectionStorageTest {

    @Override
    protected ProjectionStorage<ProjectId> newStorage(Class<? extends Entity<?, ?>> entityClass) {
        DataSourceWrapper dataSource =
                whichIsStoredInMemory("projectionStorageTests");
        @SuppressWarnings("unchecked") // Required for the tests.
                Class<? extends Projection<ProjectId, ?, ?>> projectionClass =
                (Class<? extends Projection<ProjectId, ?, ?>>) entityClass;
        TypeMapping typeMapping = H2_1_4;
        JdbcRecordStorage<ProjectId> entityStorage =
                JdbcRecordStorage.<ProjectId>newBuilder()
                        .setDataSource(dataSource)
                        .setMultitenant(false)
                        .setEntityClass(projectionClass)
                        .setTypeMapping(typeMapping)
                        .build();
        ProjectionStorage<ProjectId> storage =
                JdbcProjectionStorage.<ProjectId>newBuilder()
                        .setRecordStorage(entityStorage)
                        .setDataSource(dataSource)
                        .setMultitenant(false)
                        .setProjectionClass(projectionClass)
                        .setTypeMapping(typeMapping)
                        .build();
        return storage;
    }

    @Test
    @DisplayName("throw ISE when closing twice")
    void throwOnClosingTwice() {
        ProjectionStorage<?> storage = storage();
        storage.close();
        assertThrows(IllegalStateException.class, storage::close);
    }

    @Test
    @DisplayName("accept datasource in builder even though it is not used")
    void acceptDatasourceInBuilder() {
        StorageBuilder<?, ?> builder =
                JdbcProjectionStorage.newBuilder()
                                     .setDataSource(whichIsStoredInMemory(newUuid()));
        assertNotNull(builder);
    }

    @Test
    @DisplayName("require non-null projection class")
    void rejectNullProjectionClass() {
        Class<? extends Projection<Object, ?, ?>> nullClass = nullRef();
        assertThrows(NullPointerException.class,
                     () -> JdbcProjectionStorage.newBuilder()
                                                .setProjectionClass(nullClass));
    }

    @Test
    @DisplayName("require non-null record storage")
    void rejectNullRecordStorage() {
        JdbcRecordStorage<Object> nullStorage = nullRef();
        assertThrows(NullPointerException.class,
                     () -> JdbcProjectionStorage.newBuilder()
                                                .setRecordStorage(nullStorage));
    }

    @Override
    protected Class<? extends TestCounterEntity> getTestEntityClass() {
        return TestCounterEntity.class;
    }
}
