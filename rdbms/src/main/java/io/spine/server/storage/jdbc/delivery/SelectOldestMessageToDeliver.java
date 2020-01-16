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

import com.google.protobuf.Descriptors;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.sql.AbstractSQLQuery;
import io.spine.server.delivery.InboxMessage;

import static com.querydsl.core.types.Order.DESC;
import static io.spine.server.delivery.InboxMessageStatus.TO_DELIVER;
import static io.spine.server.storage.jdbc.delivery.InboxTable.Column.SHARD_INDEX;
import static io.spine.server.storage.jdbc.delivery.InboxTable.Column.WHEN_RECEIVED;
import static io.spine.server.storage.jdbc.delivery.InboxTable.Column.WHEN_RECEIVED_NANOS;
import static io.spine.server.storage.jdbc.message.MessageTable.bytesColumn;

/**
 * Selects the oldest {@code InboxMessage} in
 * {@link io.spine.server.delivery.InboxMessageStatus#TO_DELIVER TO_DELIVER} status.
 */
public class SelectOldestMessageToDeliver extends SelectByShardIndexQuery<InboxMessage> {

    private SelectOldestMessageToDeliver(Builder builder) {
        super(builder);
    }

    @Override
    protected AbstractSQLQuery<Object, ?> query() {
        OrderSpecifier<Comparable> bySeconds = orderBy(WHEN_RECEIVED, DESC);
        OrderSpecifier<Comparable> byNanos = orderBy(WHEN_RECEIVED_NANOS, DESC);
        return factory().select(pathOf(bytesColumn()))
                        .from(table())
                        .where(pathOf(SHARD_INDEX).eq(shardIndex()),
                               pathOf(InboxTable.Column.STATUS).eq(TO_DELIVER.toString()))
                        .orderBy(bySeconds, byNanos)
                        .limit(1);

    }

    @Override
    protected Descriptors.Descriptor messageDescriptor() {
        return InboxMessage.getDescriptor();
    }

    static Builder newBuilder() {
        return new Builder();
    }

    static class Builder
            extends SelectByShardIndexQuery.Builder<Builder, SelectOldestMessageToDeliver> {

        @Override
        protected Builder getThis() {
            return this;
        }

        @Override
        protected SelectOldestMessageToDeliver doBuild() {
            return new SelectOldestMessageToDeliver(this);
        }
    }
}
