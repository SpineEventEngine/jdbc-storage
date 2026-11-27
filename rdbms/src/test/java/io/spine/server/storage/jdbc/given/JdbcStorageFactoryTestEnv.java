/*
 * Copyright 2026 CodeMatters, Lda.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package io.spine.server.storage.jdbc.given;

import com.google.common.collect.ImmutableMap;
import io.spine.base.Identifier;
import io.spine.server.ContextSpec;
import io.spine.server.delivery.Delivery;
import io.spine.server.delivery.InboxColumn;
import io.spine.server.delivery.InboxMessage;
import io.spine.server.delivery.InboxMessageId;
import io.spine.server.delivery.InboxSignalId;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.storage.ColumnTypeMapping;
import io.spine.server.storage.RecordSpec;
import io.spine.server.storage.jdbc.DataSourceConfig;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.JdbcStorageFactory;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.record.JdbcRecordStorage;
import io.spine.server.storage.jdbc.type.JdbcColumnMapping;
import io.spine.server.storage.jdbc.type.JdbcColumnTypeMapping;
import io.spine.test.storage.StgProject;
import io.spine.test.storage.StgProjectId;

import static io.spine.server.storage.jdbc.GivenDataSource.prefix;
import static io.spine.server.storage.jdbc.PredefinedMapping.H2_2_5;
import static io.spine.server.storage.jdbc.Type.LONG;

public final class JdbcStorageFactoryTestEnv {

    /** Prevents instantiation of this utility class. */
    private JdbcStorageFactoryTestEnv() {
    }

    public static JdbcStorageFactory newFactory() {
        var dataSource = dataSource();
        return JdbcStorageFactory.newBuilder()
                .setDataSource(dataSource)
                // The data source is an in-memory H2 database, so the H2 mapping
                // must be used. The MySQL mapping emits a MySQL-specific binary
                // collation for string columns, which H2 cannot parse.
                .setTypeMapping(H2_2_5)
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
        return ContextSpec.multiTenant(JdbcStorageFactoryTestEnv.class.getName());
    }

    public static ContextSpec singleTenantSpec() {
        return ContextSpec.singleTenant(JdbcStorageFactoryTestEnv.class.getName());
    }

    public static JdbcRecordStorage<InboxMessageId, InboxMessage>
    newInboxStorage(DataSourceWrapper dataSource, String tableName) {
        var factory = JdbcStorageFactory
                .newBuilder()
                .setDataSource(dataSource)
                .setTypeMapping(H2_2_5)
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

    /**
     * A test-only custom type mapping for the record table storing {@code InboxMessage}s.
     */
    public static class InboxMessageColumnMapping extends JdbcColumnMapping {

        @Override
        protected ImmutableMap<Class<?>, JdbcColumnTypeMapping<?, ?>> customRules() {
            var signalMapping =
                    new JdbcColumnTypeMapping<InboxSignalId, Long>(
                            (signalId) -> (long) signalId.hashCode(),
                            LONG);
            return ImmutableMap.of(
                    InboxSignalId.class, signalMapping
            );
        }
    }

    /**
     * A test-only aggregate for testing the JDBC storage factory features.
     */
    public static class StgProjectAggregate
            extends Aggregate<StgProjectId, StgProject, StgProject.Builder> {
    }
}
