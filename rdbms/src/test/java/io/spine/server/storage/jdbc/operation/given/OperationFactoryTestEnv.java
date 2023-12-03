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

package io.spine.server.storage.jdbc.operation.given;

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
import io.spine.server.delivery.InboxSignalId;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.JdbcStorageFactory;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.operation.OperationFactory;
import io.spine.server.storage.jdbc.operation.WriteOne;
import io.spine.server.storage.jdbc.query.InsertOneQuery;
import io.spine.server.storage.jdbc.record.JdbcRecord;
import io.spine.server.storage.jdbc.record.RecordTable;
import io.spine.test.storage.StgProject;
import io.spine.type.TypeUrl;

import static io.spine.base.Identifier.newUuid;
import static io.spine.base.Time.currentTime;
import static io.spine.server.delivery.InboxLabel.CATCH_UP;
import static io.spine.server.delivery.InboxLabel.HANDLE_COMMAND;
import static io.spine.server.delivery.InboxMessageStatus.DELIVERED;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;

public final class OperationFactoryTestEnv {

    /**
     * Prevents this test environment from direct instantiation.
     */
    private OperationFactoryTestEnv() {
    }

    /**
     * An inbox label value to use in the scope of the custom {@link InboxWriteOne} operation.
     */
    public static final InboxLabel OVERRIDDEN_LABEL = CATCH_UP;

    /**
     * Creates a new builder for the {@code JdbcStorageFactory},
     * which is configured to use in-memory structures for data.
     */
    public static JdbcStorageFactory.Builder imStorageFactoryBuilder() {
        return JdbcStorageFactory
                .newBuilder()
                .setDataSource(whichIsStoredInMemory(OperationFactoryTestEnv.class.getName()));
    }

    /**
     * Creates a new {code InboxMessage} with {@link InboxLabel#HANDLE_COMMAND HANDLE_COMMAND}
     * inbox label.
     */
    public static InboxMessage randomHandleCommandMessage() {
        var index = DeliveryStrategy.newIndex(0, 1);
        var id = InboxMessageMixin.generateIdWith(index);
        var signalId = InboxSignalId.newBuilder()
                .setValue("some-command-id");
        var result = InboxMessage.newBuilder()
                .setId(id)
                .setStatus(DELIVERED)
                .setCommand(Command.getDefaultInstance())
                .setInboxId(randomInboxId())
                .setSignalId(signalId)
                .setLabel(HANDLE_COMMAND)
                .setWhenReceived(currentTime())
                .setVersion(42)
                .build();
        return result;
    }

    private static InboxId randomInboxId() {
        var packedId = Identifier.pack(newUuid());
        var entityId = EntityId.newBuilder()
                .setId(packedId);
        var typeUrl = TypeUrl.of(StgProject.class)
                             .value();
        return InboxId.newBuilder()
                .setEntityId(entityId)
                .setTypeUrl(typeUrl)
                .build();
    }

    /**
     * An operation factory, which overrides the "write-one" operation
     * for {@code InboxMessage}s.
     */
    public static final class TestOperationFactory extends OperationFactory {

        public TestOperationFactory(DataSourceWrapper wrapper, TypeMapping mapping) {
            super(wrapper, mapping);
        }

        @Override
        public <I, R extends Message> WriteOne<I, R> writeOne(RecordTable<I, R> table) {
            return new InboxWriteOne<>(table, dataSource());
        }
    }

    /**
     * A {@code WriteOne} operation, which sets a custom {@link #OVERRIDDEN_LABEL} inbox label
     * if and only if an instance of {@code InboxMessage} is being written.
     *
     * <p>For all other types of records written uses the default {@code WriteOne} behaviour.
     *
     * @param <I>
     *         type of record identifiers for the records being written
     * @param <R>
     *         type of written records
     */
    private static final class InboxWriteOne<I, R extends Message> extends WriteOne<I, R> {

        InboxWriteOne(RecordTable<I, R> table, DataSourceWrapper dataSource) {
            super(table, dataSource);
        }

        @Override
        public void execute(JdbcRecord<I, R> record) {
            if (record.original()
                      .record() instanceof InboxMessage) {
                modifyAndWrite(record);
            } else {
                super.execute(record);
            }
        }

        @SuppressWarnings("unchecked")
        private void modifyAndWrite(JdbcRecord<I, R> record) {
            var castRecord = (JdbcRecord<InboxMessageId, InboxMessage>) record;
            var originalMessage = castRecord.original()
                                            .record();
            var modifiedMessage = originalMessage
                    .toBuilder()
                    .setLabel(OVERRIDDEN_LABEL)
                    .build();
            var modifiedRecord = castRecord.copyWithRecord(
                    modifiedMessage);
            var castTable = (RecordTable<InboxMessageId, InboxMessage>) table();
            var query = InsertOneQuery
                    .<InboxMessageId, InboxMessage>newBuilder()
                    .setTableSpec(castTable.spec())
                    .setDataSource(dataSource())
                    .setRecord(modifiedRecord)
                    .build();
            query.execute();
        }
    }
}
