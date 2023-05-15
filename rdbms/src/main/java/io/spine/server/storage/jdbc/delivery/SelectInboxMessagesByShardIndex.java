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

package io.spine.server.storage.jdbc.delivery;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.sql.AbstractSQLQuery;
import io.spine.server.delivery.InboxMessage;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.querydsl.core.types.Order.ASC;
import static io.spine.server.storage.jdbc.delivery.InboxTable.Column.SHARD_INDEX;
import static io.spine.server.storage.jdbc.delivery.InboxTable.Column.WHEN_RECEIVED;
import static io.spine.server.storage.jdbc.message.MessageTable.bytesColumn;
import static java.util.Objects.requireNonNull;

/**
 * Selects messages from the {@link InboxTable} that belong to a shard at a given
 * {@link io.spine.server.delivery.ShardIndex index}.
 *
 * <p>Messages are ordered from oldest to newest.
 */
final class SelectInboxMessagesByShardIndex extends SelectByShardIndexQuery<InboxMessage> {

    private final @Nullable Timestamp sinceWhen;
    private final int pageSize;

    private SelectInboxMessagesByShardIndex(Builder builder) {
        super(builder);
        this.sinceWhen = builder.sinceWhen;
        this.pageSize = builder.pageSize;
    }

    @Override
    @SuppressWarnings("rawtypes")    /* For simplicity. */
    protected AbstractSQLQuery<Object, ?> query() {
        OrderSpecifier<Comparable> byTime = orderBy(WHEN_RECEIVED, ASC);
        AbstractSQLQuery<Object, ?> partial =
                factory().select(pathOf(bytesColumn()))
                         .from(table())
                         .where(pathOf(SHARD_INDEX).eq(shardIndex()));
        if (sinceWhen != null) {
            partial.where(comparablePathOf(WHEN_RECEIVED, Long.class)
                                  .gt(Timestamps.toNanos(sinceWhen)));
        }
        AbstractSQLQuery<Object, ?> result = partial.orderBy(byTime)
                                                    .limit(pageSize);
        return result;
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

        private @Nullable Timestamp sinceWhen;

        private int pageSize;

        @Override
        protected Builder getThis() {
            return this;
        }

        @Override
        protected SelectInboxMessagesByShardIndex doBuild() {
            return new SelectInboxMessagesByShardIndex(this);
        }

        /**
         * Sets the point in time, since which the messages should be selected
         * according to their {@linkplain InboxTable.Column#WHEN_RECEIVED "when received"} time.
         *
         * <p>This value is exclusive, meaning all returned messages will have
         * their receiving time greater than the value specified.
         */
        void setSinceWhen(Timestamp sinceWhen) {
            this.sinceWhen = requireNonNull(sinceWhen);
        }

        /**
         * Specifies the page size for the returning list of messages.
         *
         * <p>The specified value must be positive.
         */
        void setPageSize(int size) {
            checkArgument(size > 0);
            this.pageSize = size;
        }
    }
}
