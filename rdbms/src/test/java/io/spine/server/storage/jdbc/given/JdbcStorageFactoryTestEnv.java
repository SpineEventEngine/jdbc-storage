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

package io.spine.server.storage.jdbc.given;

import io.spine.base.Identifier;
import io.spine.server.ContextSpec;
import io.spine.server.delivery.Delivery;
import io.spine.server.delivery.InboxColumn;
import io.spine.server.delivery.InboxMessage;
import io.spine.server.delivery.InboxMessageId;
import io.spine.server.storage.ColumnTypeMapping;
import io.spine.server.storage.RecordSpec;
import io.spine.server.storage.jdbc.DataSourceConfig;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.JdbcStorageFactory;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.record.JdbcRecordStorage;
import io.spine.server.storage.jdbc.type.JdbcColumnMapping;

import static io.spine.server.storage.jdbc.GivenDataSource.prefix;
import static io.spine.server.storage.jdbc.PredefinedMapping.H2_2_1;
import static io.spine.server.storage.jdbc.PredefinedMapping.MYSQL_5_7;

public final class JdbcStorageFactoryTestEnv {

    /** Prevents instantiation of this utility class. */
    private JdbcStorageFactoryTestEnv() {
    }

    public static JdbcStorageFactory newFactory() {
        var dataSource = dataSource();
        return JdbcStorageFactory.newBuilder()
                                 .setDataSource(dataSource)
                                 .setTypeMapping(MYSQL_5_7)
                                 .build();
    }

    private static DataSourceConfig dataSource() {
        return DataSourceConfig.newBuilder()
                .setJdbcUrl(prefix("factoryTests-" + Identifier.newUuid()))
                .setUsername("SA")
                .setPassword("pwd")
                .setMaxPoolSize(30)
                .build();
    }

    public static ContextSpec multitenantSpec() {
        return ContextSpec.multitenant(JdbcStorageFactoryTestEnv.class.getName());
    }

    public static ContextSpec singleTenantSpec() {
        return ContextSpec.singleTenant(JdbcStorageFactoryTestEnv.class.getName());
    }

    public static JdbcRecordStorage<InboxMessageId, InboxMessage>
    newInboxStorage(DataSourceWrapper dataSource, String tableName) {
        var factory = JdbcStorageFactory
                .newBuilder()
                .setDataSource(dataSource)
                .setTypeMapping(H2_2_1)
                .setTableName(InboxMessage.class, tableName)
                .build();
        var storage = (JdbcRecordStorage<InboxMessageId, InboxMessage>)
                factory.createRecordStorage(deliveryContextSpec(), inboxMessageSpec());
        return storage;
    }

    public static ContextSpec deliveryContextSpec() {
        return Delivery.contextSpec(false);
    }

    public static RecordSpec<InboxMessageId, InboxMessage> inboxMessageSpec() {
        var inboxMessageSpec = new RecordSpec<>(
                InboxMessageId.class,
                InboxMessage.class,
                InboxMessage::getId,
                InboxColumn.definitions());
        return inboxMessageSpec;
    }

    public static class TestColumnMapping extends JdbcColumnMapping {

        @Override
        public Type typeOf(Class<?> columnType) {
            return Type.STRING;
        }

        @Override
        public <T> ColumnTypeMapping<T, ? extends String> of(Class<T> type) {
            return o -> "always-the-same-string";
        }

        @Override
        public ColumnTypeMapping<?, ? extends String> ofNull() {
            return o -> "the-null";
        }
    }
}
