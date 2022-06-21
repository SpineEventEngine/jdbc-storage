/*
 * Copyright 2020, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc;

import io.spine.server.aggregate.AggregateStorage;
import io.spine.server.delivery.InboxStorage;
import io.spine.server.projection.ProjectionStorage;
import io.spine.server.storage.given.RecordStorageTestEnv.TestCounterEntity;
import io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.TestAggregate;
import io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.TestColumnMapping;
import io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.TestProjection;
import io.spine.server.storage.jdbc.record.JdbcRecordStorage;
import io.spine.server.storage.jdbc.type.DefaultJdbcColumnMapping;
import io.spine.server.storage.jdbc.type.JdbcColumnMapping;
import io.spine.test.storage.ProjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.jdbc.GivenDataSource.whichHoldsMetadata;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.PredefinedMapping.H2_2_1;
import static io.spine.server.storage.jdbc.PredefinedMapping.MYSQL_5_7;
import static io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.multitenantSpec;
import static io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.newFactory;
import static io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.singletenantSpec;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("DuplicateStringLiteralInspection") // Common test display names.
@DisplayName("JdbcStorageFactory should")
class JdbcStorageFactoryTest {

    @Test
    @DisplayName("allow to use custom data source")
    void allowCustomDataSource() {
        JdbcStorageFactory factory = JdbcStorageFactory
                .newBuilder()
                .setDataSource(whichIsStoredInMemory(newUuid()))
                .setTypeMapping(H2_2_1)
                .build();

        assertNotNull(factory);
    }

    @Test
    @DisplayName("allow to set custom column mapping")
    void setColumnMapping() {
        JdbcColumnMapping<String> columnMapping = new TestColumnMapping();
        JdbcStorageFactory factory = JdbcStorageFactory
                .newBuilder()
                .setDataSource(whichIsStoredInMemory(newUuid()))
                .setColumnMapping(columnMapping)
                .build();
        JdbcColumnMapping<?> mappingFromFactory = factory.columnMapping();

        assertThat(mappingFromFactory).isEqualTo(columnMapping);
    }

    @Test
    @DisplayName("have `DefaultJdbcColumnMapping` by default")
    void haveDefaultColumnMapping() {
        JdbcStorageFactory factory = newFactory();
        JdbcColumnMapping<?> columnMapping = factory.columnMapping();

        assertThat(columnMapping).isInstanceOf(DefaultJdbcColumnMapping.class);
    }

    @Nested
    @DisplayName("create record storage")
    class CreateRecordStorage {

        @Test
        @DisplayName("which is multitenant")
        void multitenant() {
            JdbcStorageFactory factory = newFactory();
            JdbcRecordStorage<ProjectId> storage =
                    factory.createRecordStorage(multitenantSpec(), TestCounterEntity.class);
            assertTrue(storage.isMultitenant());
        }

        @Test
        @DisplayName("which is single tenant")
        void singleTenant() {
            JdbcStorageFactory factory = newFactory();
            JdbcRecordStorage<ProjectId> storage =
                    factory.createRecordStorage(singletenantSpec(), TestCounterEntity.class);
            assertFalse(storage.isMultitenant());
        }
    }

    @Nested
    @DisplayName("create aggregate storage")
    class CreateAggregateStorage {

        @Test
        @DisplayName("which is multitenant")
        void multitenant() {
            JdbcStorageFactory factory = newFactory();
            AggregateStorage<String> storage =
                    factory.createAggregateStorage(multitenantSpec(), TestAggregate.class);
            assertTrue(storage.isMultitenant());
        }

        @Test
        @DisplayName("which is single tenant")
        void singleTenant() {
            JdbcStorageFactory factory = newFactory();
            AggregateStorage<String> storage =
                    factory.createAggregateStorage(singletenantSpec(), TestAggregate.class);
            assertFalse(storage.isMultitenant());
        }
    }

    @Nested
    @DisplayName("create projection storage")
    class CreateProjectionStorage {

        @Test
        @DisplayName("which is multitenant")
        void multitenant() {
            JdbcStorageFactory factory = newFactory();
            ProjectionStorage<String> storage =
                    factory.createProjectionStorage(multitenantSpec(), TestProjection.class);
            assertTrue(storage.isMultitenant());
        }

        @Test
        @DisplayName("which is single tenant")
        void singleTenant() {
            JdbcStorageFactory factory = newFactory();
            ProjectionStorage<String> storage =
                    factory.createProjectionStorage(singletenantSpec(), TestProjection.class);
            assertFalse(storage.isMultitenant());
        }
    }

    @Test
    @DisplayName("create inbox storage")
    void createInboxStorage() {
        JdbcStorageFactory factory = newFactory();
        InboxStorage inboxStorage = factory.createInboxStorage(false);
        assertFalse(inboxStorage.isMultitenant());
    }

    @Test
    @DisplayName("close datastore on close")
    void closeDatastoreOnClose() {
        DataSourceWrapper dataSource = whichIsStoredInMemory(newUuid());
        JdbcStorageFactory factory = JdbcStorageFactory
                .newBuilder()
                .setDataSource(dataSource)
                .setTypeMapping(H2_2_1)
                .build();
        factory.close();
        assertThat(dataSource.isClosed())
                .isTrue();
    }

    @Test
    @DisplayName("select mapping based on the given data source")
    void selectMapping() {
        DataSourceWrapper dataSource = whichHoldsMetadata(
                MYSQL_5_7.getDatabaseProductName(),
                MYSQL_5_7.getMajorVersion(),
                MYSQL_5_7.getMinorVersion());
        JdbcStorageFactory factory = JdbcStorageFactory
                .newBuilder()
                .setDataSource(dataSource)
                .build();
        assertEquals(MYSQL_5_7, factory.getTypeMapping());
    }
}
