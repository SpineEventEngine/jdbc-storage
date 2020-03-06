/*
 * Copyright 2020, TeamDev. All rights reserved.
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
import com.google.common.collect.Iterators;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.util.Timestamps;
import io.spine.server.delivery.InboxMessage;
import io.spine.server.delivery.InboxMessageId;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.message.MessageTable;
import io.spine.server.storage.jdbc.query.IdColumn;
import io.spine.string.Stringifiers;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.Optional;

import static io.spine.server.storage.jdbc.Type.BOOLEAN;
import static io.spine.server.storage.jdbc.Type.INT;
import static io.spine.server.storage.jdbc.Type.LONG;
import static io.spine.server.storage.jdbc.Type.STRING;
import static io.spine.server.storage.jdbc.Type.STRING_255;

/**
 * A table in the DB responsible for storing the {@link io.spine.server.delivery.Inbox Inbox} data.
 *
 * <p>Basically stores the {@linkplain InboxMessage inbox messages} on a field-by-field basis in
 * the appropriate SQL format.
 */
final class InboxTable extends MessageTable<InboxMessageId, InboxMessage> {

    private static final String NAME = "inbox";

    InboxTable(DataSourceWrapper dataSource, TypeMapping typeMapping) {
        super(NAME, IdColumn.of(Column.ID, InboxMessageId.class), dataSource, typeMapping);
    }

    @Override
    protected Descriptor messageDescriptor() {
        return InboxMessage.getDescriptor();
    }

    @Override
    protected Iterable<Column> messageSpecificColumns() {
        return ImmutableList.copyOf(Column.values());
    }

    /**
     * Obtains all inbox messages that belong to a specified {@code shardIndex}.
     *
     * <p>The messages are ordered from oldest to newest.
     */
    Iterator<InboxMessage> readAll(ShardIndex index) {
        SelectInboxMessagesByShardIndex query = composeSelectByShardIndexQuery(index);
        Iterator<InboxMessage> iterator = query.execute();
        return iterator;
    }

    /**
     * Obtains the oldest message in {@link io.spine.server.delivery.InboxMessageStatus#TO_DELIVER
     * TO_DELIVER} status from the shard with the given {@code index}.
     *
     * <p>If there are no such messages in the specified shard, an {@code Optional.empty()}
     * is returned.
     */
    Optional<InboxMessage> readOldestToDeliver(ShardIndex index) {
        SelectOldestMessageToDeliver query =
                SelectOldestMessageToDeliver.newBuilder()
                                            .setDataSource(dataSource())
                                            .setTableName(name())
                                            .setShardIndex(index)
                                            .build();
        Iterator<InboxMessage> result = query.execute();
        InboxMessage first = Iterators.getOnlyElement(result, null);
        return Optional.ofNullable(first);
    }

    private SelectInboxMessagesByShardIndex composeSelectByShardIndexQuery(ShardIndex index) {
        SelectInboxMessagesByShardIndex.Builder builder =
                SelectInboxMessagesByShardIndex.newBuilder();
        SelectInboxMessagesByShardIndex query = builder.setTableName(name())
                                                       .setDataSource(dataSource())
                                                       .setShardIndex(index)
                                                       .build();
        return query;
    }

    /**
     * The columns of {@link InboxMessage} DB representation.
     */
    @SuppressWarnings("ProtoTimestampGetSecondsGetNano") // `getNanos()` method is used on purpose.
    enum Column implements MessageTable.Column<InboxMessage> {

        ID(InboxMessage::getId),

        SIGNAL_ID(STRING_255, m -> m.getSignalId()
                                    .getValue()),

        INBOX_ID(STRING_255, m -> Stringifiers.toString(m.getInboxId())),

        SHARD_INDEX(LONG, m -> m.getId()
                                .getIndex()
                                .getIndex()),

        OF_TOTAL_SHARDS(LONG, m -> m.getId()
                                    .getIndex()
                                    .getOfTotal()),

        IS_EVENT(BOOLEAN, InboxMessage::hasEvent),

        IS_COMMAND(BOOLEAN, InboxMessage::hasCommand),

        LABEL(STRING, m -> m.getLabel()
                            .toString()),

        STATUS(STRING, m -> m.getStatus()
                             .toString()),

        WHEN_RECEIVED(LONG, m -> Timestamps.toNanos(m.getWhenReceived())),

        VERSION(INT, InboxMessage::getVersion);

        private final @Nullable Type type;
        private final Getter<InboxMessage> getter;

        Column(Type type, Getter<InboxMessage> getter) {
            this.type = type;
            this.getter = getter;
        }

        Column(Getter<InboxMessage> getter) {
            this.type = null;
            this.getter = getter;
        }

        @Override
        public @Nullable Type type() {
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
