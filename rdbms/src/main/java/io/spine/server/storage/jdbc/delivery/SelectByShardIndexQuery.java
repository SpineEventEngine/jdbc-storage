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

import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.sql.AbstractSQLQuery;
import io.spine.server.delivery.InboxMessage;
import io.spine.server.delivery.ShardIndex;
import io.spine.server.storage.jdbc.query.AbstractQuery;
import io.spine.server.storage.jdbc.query.DbIterator;
import io.spine.server.storage.jdbc.query.SelectQuery;

import java.sql.ResultSet;

import static com.querydsl.core.types.Order.ASC;
import static io.spine.server.storage.jdbc.delivery.InboxMessageTable.Column.SHARD_INDEX;
import static io.spine.server.storage.jdbc.delivery.InboxMessageTable.Column.WHEN_RECEIVED;
import static io.spine.server.storage.jdbc.delivery.InboxMessageTable.Column.WHEN_RECEIVED_NANOS;
import static io.spine.server.storage.jdbc.message.MessageTable.bytesColumn;
import static io.spine.server.storage.jdbc.query.ColumnReaderFactory.messageReader;

final class SelectByShardIndexQuery
        extends AbstractQuery
        implements SelectQuery<DbIterator<InboxMessage>> {

    private final ShardIndex shardIndex;

    private SelectByShardIndexQuery(Builder builder) {
        super(builder);
        this.shardIndex = builder.shardIndex;
    }

    @Override
    public DbIterator<InboxMessage> execute() {
        ResultSet resultSet = query().getResults();
        DbIterator<InboxMessage> iterator =
                DbIterator.over(resultSet,
                                messageReader(bytesColumn().name(), InboxMessage.getDescriptor()));
        return iterator;
    }

    private AbstractSQLQuery<Object, ?> query() {
        OrderSpecifier<Comparable> bySeconds = orderBy(WHEN_RECEIVED, ASC);
        OrderSpecifier<Comparable> byNanos = orderBy(WHEN_RECEIVED_NANOS, ASC);
        return factory().select(pathOf(bytesColumn()))
                        .from(table())
                        .where(pathOf(SHARD_INDEX).eq(shardIndex))
                        .orderBy(bySeconds, byNanos);
    }

    static Builder newBuilder() {
        return new Builder();
    }

    static class Builder extends AbstractQuery.Builder<Builder, SelectByShardIndexQuery> {

        private ShardIndex shardIndex;

        Builder setShardIndex(ShardIndex shardIndex) {
            this.shardIndex = shardIndex;
            return getThis();
        }

        @Override
        protected Builder getThis() {
            return this;
        }

        @Override
        protected SelectByShardIndexQuery doBuild() {
            return new SelectByShardIndexQuery(this);
        }
    }
}
