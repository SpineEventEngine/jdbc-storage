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

package io.spine.server.storage.jdbc.operation;

import com.google.protobuf.Message;
import io.spine.base.Identifier;
import io.spine.client.EntityId;
import io.spine.core.Command;
import io.spine.server.delivery.DeliveryStrategy;
import io.spine.server.delivery.InboxId;
import io.spine.server.delivery.InboxLabel;
import io.spine.server.delivery.InboxMessage;
import io.spine.server.delivery.InboxMessageId;
import io.spine.server.delivery.InboxMessageMixin;
import io.spine.server.delivery.InboxMessageStatus;
import io.spine.server.delivery.InboxSignalId;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.JdbcStorageFactory;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.query.InsertOneQuery;
import io.spine.server.storage.jdbc.record.JdbcRecord;
import io.spine.server.storage.jdbc.record.JdbcRecordStorage;
import io.spine.server.storage.jdbc.record.RecordTable;
import io.spine.test.storage.StgProject;
import io.spine.type.TypeUrl;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.base.Identifier.newUuid;
import static io.spine.base.Time.currentTime;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.deliveryContextSpec;
import static io.spine.server.storage.jdbc.given.JdbcStorageFactoryTestEnv.inboxMessageSpec;

@DisplayName("With `OperationFactory` it should be possible")
final class OperationFactoryTest {

    private static final String MAGIC_ID = "2128506";

    @Test
    @DisplayName("use custom operation instead of a default one")
    void useCustomOperations() {
        var factory = imStorageFactoryBuilder()
                .useOperationFactory(TestOperationFactory::new)
                .build();
        var storage = (JdbcRecordStorage<InboxMessageId, InboxMessage>)
                factory.createRecordStorage(deliveryContextSpec(), inboxMessageSpec());

        var message = randomInboxMessage();
        storage.write(message.getId(), message);
        var allIds = ImmutableList.copyOf(storage.index());
        assertThat(allIds.size()).isEqualTo(1);
        var actual = allIds.get(0);
        assertThat(actual.getUuid())
                .contains(MAGIC_ID);
    }

    private static JdbcStorageFactory.Builder imStorageFactoryBuilder() {
        return JdbcStorageFactory
                .newBuilder()
                .setDataSource(whichIsStoredInMemory(newUuid()));
    }

    private static class TestOperationFactory extends OperationFactory {

        private TestOperationFactory(DataSourceWrapper wrapper, TypeMapping mapping) {
            super(wrapper, mapping);
        }

        @Override
        public <I, R extends Message> WriteOne<I, R> writeOne(RecordTable<I, R> table) {
            return new InboxWriteOne<>(table, dataSource());
        }
    }

    private static InboxMessage randomInboxMessage() {
        var index = DeliveryStrategy.newIndex(0, 1);
        var id = InboxMessageMixin.generateIdWith(index);
        var signalId = InboxSignalId.newBuilder()
                .setValue("some-command-id");
        var result = InboxMessage.newBuilder()
                .setId(id)
                .setStatus(InboxMessageStatus.DELIVERED)
                .setCommand(Command.getDefaultInstance())
                .setInboxId(randomInboxId())
                .setSignalId(signalId)
                .setLabel(InboxLabel.HANDLE_COMMAND)
                .setWhenReceived(currentTime())
                .setVersion(42)
                .build();
        return result;
    }

    private static InboxId randomInboxId() {
        var packedId = Identifier.pack(newUuid());
        return InboxId.newBuilder()
                .setEntityId(EntityId.newBuilder()
                                     .setId(packedId)
                                     .build())
                .setTypeUrl(TypeUrl.of(StgProject.class).value())
                .build();
    }

    private static class InboxWriteOne<I, R extends Message> extends WriteOne<I, R> {

        InboxWriteOne(RecordTable<I, R> table, DataSourceWrapper dataSource) {
            super(table, dataSource);
        }

        @Override
        public void execute(JdbcRecord<I, R> record) {
            if(record.id() instanceof InboxMessageId) {
                modifyAndWrite(record);
            } else {
                super.execute(record);
            }
        }

        @SuppressWarnings("unchecked")
        private void modifyAndWrite(JdbcRecord<I, R> record) {
            var castId = (InboxMessageId) record.id();
            var modifiedId = castId.toBuilder()
                    .setUuid(MAGIC_ID)
                    .build();
            var castRecord = (JdbcRecord<InboxMessageId, InboxMessage>) record;
            var castTable = (RecordTable<InboxMessageId, InboxMessage>) table();

            var query = InsertOneQuery
                    .<InboxMessageId, InboxMessage>newBuilder()
                    .setTableSpec(castTable.spec())
                    .setDataSource(dataSource())
                    .setRecord(castRecord)
                    .build();
            query.execute();
        }
    }
}
