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

import com.google.protobuf.Descriptors.Descriptor;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.sql.AbstractSQLQuery;
import io.spine.server.delivery.InboxMessage;

import static com.querydsl.core.types.Order.ASC;
import static io.spine.server.storage.jdbc.delivery.InboxTable.Column.SHARD_INDEX;
import static io.spine.server.storage.jdbc.delivery.InboxTable.Column.WHEN_RECEIVED;
import static io.spine.server.storage.jdbc.delivery.InboxTable.Column.WHEN_RECEIVED_NANOS;
import static io.spine.server.storage.jdbc.message.MessageTable.bytesColumn;

/**
 * Selects messages from the {@link InboxTable} at a given
 * {@link io.spine.server.delivery.ShardIndex ShardIndex}.
 *
 * <p>Messages are ordered from oldest to newest.
 */
final class SelectInboxMessagesByShardIndex extends SelectByShardIndexQuery<InboxMessage> {

    private SelectInboxMessagesByShardIndex(Builder builder) {
        super(builder);
    }

    @Override
    protected AbstractSQLQuery<Object, ?> query() {
        OrderSpecifier<Comparable> bySeconds = orderBy(WHEN_RECEIVED, ASC);
        OrderSpecifier<Comparable> byNanos = orderBy(WHEN_RECEIVED_NANOS, ASC);
        return factory().select(pathOf(bytesColumn()))
                        .from(table())
                        .where(pathOf(SHARD_INDEX).eq(shardIndex()))
                        .orderBy(bySeconds, byNanos);
    }

    @Override
    protected Descriptor messageDescriptor() {
        return InboxMessage.getDescriptor();
    }

    static Builder newBuilder() {
        return new Builder();
    }

    static class Builder
            extends SelectByShardIndexQuery.Builder<Builder, SelectInboxMessagesByShardIndex> {

        @Override
        protected Builder getThis() {
            return this;
        }

        @Override
        protected SelectInboxMessagesByShardIndex doBuild() {
            return new SelectInboxMessagesByShardIndex(this);
        }
    }
}
