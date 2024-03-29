/*
 * Copyright 2023, TeamDev. All rights reserved.
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

import io.spine.server.ContextSpec;
import io.spine.server.delivery.InboxMessage;
import io.spine.server.delivery.InboxMessageId;
import io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.InboxMessageColumnMapping;
import io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.StgProjectProjection;
import io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.TestColumnMapping;
import io.spine.server.storage.jdbc.record.JdbcRecordStorage;
import io.spine.server.storage.jdbc.record.TableNames;
import io.spine.server.storage.jdbc.type.JdbcColumnMapping;
import io.spine.test.storage.StgProject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.given.GivenStorageProject.messageSpec;
import static io.spine.server.storage.jdbc.GivenDataSource.whichHoldsMetadata;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.PredefinedMapping.H2_2_1;
import static io.spine.server.storage.jdbc.PredefinedMapping.MYSQL_5_7;
import static io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.deliveryContextSpec;
import static io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.inboxMessageSpec;
import static io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.multitenantSpec;
import static io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.newFactory;
import static io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.newInboxStorage;
import static io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.singleTenantSpec;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("`JdbcStorageFactory` should")
class JdbcStorageFactoryTest {

    @Test
    @DisplayName("allow to use custom data source")
    void allowCustomDataSource() {
        var factory = JdbcStorageFactory
                .newBuilder()
                .setDataSource(whichIsStoredInMemory(newUuid()))
                .setTypeMapping(H2_2_1)
                .build();

        assertNotNull(factory);
    }

    @Test
    @DisplayName("allow to set custom column mapping")
    void setColumnMapping() {
        var columnMapping = new TestColumnMapping();
        var factory = JdbcStorageFactory
                .newBuilder()
                .setDataSource(whichIsStoredInMemory(newUuid()))
                .setColumnMapping(columnMapping)
                .build();
        var mappingFromFactory = factory.columnMapping();

        assertThat(mappingFromFactory).isEqualTo(columnMapping);
    }

    @Test
    @DisplayName("allow to print SQL to create an RDBMS table for a certain entity")
    void printTableCreationSql() {
        var factory = JdbcStorageFactory
                .newBuilder()
                .setDataSource(whichIsStoredInMemory(newUuid()))
                .build();
        var createTableSql = factory.tableCreationSql(ContextSpec.singleTenant("SQLs"),
                                                      StgProjectProjection.class);
        assertThat(createTableSql)
                .isNotEmpty();
        assertThat(createTableSql)
                .contains(Sql.Query.CREATE_TABLE.toString());
        assertThat(createTableSql)
                .contains(TableNames.of(StgProject.class));
    }

    @Test
    @DisplayName("have `DefaultJdbcColumnMapping` by default")
    void haveDefaultColumnMapping() {
        var factory = newFactory();
        var columnMapping = factory.columnMapping();

        assertThat(columnMapping).isInstanceOf(JdbcColumnMapping.class);
    }

    @Test
    @DisplayName("select mapping based on the given data source")
    void selectMapping() {
        var dataSource = whichHoldsMetadata(
                MYSQL_5_7.getDatabaseProductName(),
                MYSQL_5_7.getMajorVersion(),
                MYSQL_5_7.getMinorVersion());
        var factory = JdbcStorageFactory
                .newBuilder()
                .setDataSource(dataSource)
                .build();
        assertEquals(MYSQL_5_7, factory.typeMapping());
    }

    @Nested
    @DisplayName("override")
    class OverridingDefaultsPerType {

        @Test
        @DisplayName("a name for a table per record type")
        void tableName() {
            var dataSource = whichIsStoredInMemory(newUuid());
            var customName = "FooBarInboxTable";
            var storage = newInboxStorage(dataSource, customName);
            var actualName = storage.tableName();
            assertThat(actualName)
                    .isEqualTo(customName);
        }

        @Test
        @DisplayName("column mapping for a specific table per its record type")
        void columnMapping() {
            var factoryWideMapping = new TestColumnMapping();
            var inboxRecordMapping = new InboxMessageColumnMapping();
            var factory = JdbcStorageFactory
                    .newBuilder()
                    .setDataSource(whichIsStoredInMemory(newUuid()))
                    .setColumnMapping(factoryWideMapping)
                    .setCustomMapping(InboxMessage.class, inboxRecordMapping)
                    .build();

            var actualFactoryWideMapping = factory.columnMapping();
            assertThat(actualFactoryWideMapping)
                    .isEqualTo(factoryWideMapping);

            var storage = (JdbcRecordStorage<InboxMessageId, InboxMessage>)
                    factory.createRecordStorage(deliveryContextSpec(), inboxMessageSpec());
            var actualInboxMapping = storage.table()
                                            .spec()
                                            .columnMapping();
            assertThat(actualInboxMapping)
                    .isEqualTo(inboxRecordMapping);
        }
    }

    @Nested
    @DisplayName("create a record storage")
    class CreateStorage {

        @Test
        @DisplayName("which is multitenant")
        void multitenant() {
            var factory = newFactory();
            var storage =
                    factory.createRecordStorage(multitenantSpec(), messageSpec());
            assertTrue(storage.isMultitenant());
        }

        @Test
        @DisplayName("which is single tenant")
        void singleTenant() {
            var factory = newFactory();
            var storage =
                    factory.createRecordStorage(singleTenantSpec(), messageSpec());
            assertFalse(storage.isMultitenant());
        }
    }

    @Test
    @DisplayName("close datastore on close")
    void closeDatastoreOnClose() {
        var dataSource = whichIsStoredInMemory(newUuid());
        var factory = JdbcStorageFactory
                .newBuilder()
                .setDataSource(dataSource)
                .setTypeMapping(H2_2_1)
                .build();
        factory.close();
        assertThat(dataSource.isClosed())
                .isTrue();
    }
}
