/*
 * Copyright 2019, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.delivery;

import com.google.common.collect.ImmutableList;
import io.spine.server.delivery.InboxMessage;
import io.spine.server.delivery.InboxMessageId;
import io.spine.server.delivery.Page;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.message.MessageTable;
import io.spine.server.storage.jdbc.query.IdColumn;
import io.spine.server.storage.jdbc.query.SelectQuery;
import io.spine.string.Stringifiers;

import javax.annotation.Nullable;

import static io.spine.server.storage.jdbc.Type.BOOLEAN;
import static io.spine.server.storage.jdbc.Type.INT;
import static io.spine.server.storage.jdbc.Type.LONG;
import static io.spine.server.storage.jdbc.Type.STRING;
import static io.spine.server.storage.jdbc.Type.STRING_255;

/**
 * A table in the DB responsible for storing the {@link io.spine.server.delivery.Inbox Inbox} data.
 */
final class InboxMessageTable extends MessageTable<InboxMessageId, InboxMessage> {

    InboxMessageTable(String name,
                      IdColumn<InboxMessageId> idColumn,
                      DataSourceWrapper dataSource,
                      TypeMapping typeMapping) {
        super(name, idColumn, dataSource, typeMapping);
    }

    @Override
    protected ImmutableList<Column> columns() {
        return ImmutableList.copyOf(Column.values());
    }

    Page<InboxMessage> readAll(ShardIndex index) {
        return null;
    }

    private SelectQuery<InboxMessage> composeSelectByShardIndexQuery(ShardIndex index) {
        return null;
    }

    enum Column implements MessageTable.Column<InboxMessage> {

        ID(InboxMessage::getId),

        SIGNAL_ID(STRING_255, message -> message.getSignalId()
                                                .getValue()),

        INBOX_ID(STRING_255, message -> Stringifiers.toString(message.getInboxId())),

        SHARD_INDEX(LONG, message -> message.getShardIndex()
                                            .getIndex()),

        OF_TOTAL_SHARDS(LONG, message -> message.getShardIndex()
                                                .getOfTotal()),

        IS_EVENT(BOOLEAN, InboxMessage::hasEvent),

        IS_COMMAND(BOOLEAN, InboxMessage::hasCommand),

        LABEL(STRING, message -> message.getLabel()
                                        .toString()),

        STATUS(STRING, message -> message.getStatus()
                                         .toString()),

        WHEN_RECEIVED(LONG, message -> message.getWhenReceived()
                                              .getSeconds()),

        WHEN_RECEIVED_NANOS(INT, message -> message.getWhenReceived()
                                                   .getNanos());

        @Nullable
        private final Type type;
        private final Getter<InboxMessage> getter;

        Column(Type type, Getter<InboxMessage> getter) {
            this.type = type;
            this.getter = getter;
        }

        Column(Getter<InboxMessage> getter) {
            this.type = null;
            this.getter = getter;
        }

        @Nullable
        @Override
        public Type type() {
            return type;
        }

        @Override
        public boolean isPrimaryKey() {
            return this == ID;
        }

        @Override
        public boolean isNullable() {
            return false;
        }

        @Override
        public Getter<InboxMessage> getter() {
            return getter;
        }
    }
}
