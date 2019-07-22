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
import com.google.protobuf.Message;
import com.querydsl.sql.AbstractSQLQuery;
import io.spine.server.storage.jdbc.query.AbstractQuery;
import io.spine.server.storage.jdbc.query.DbIterator;
import io.spine.server.storage.jdbc.query.SelectQuery;

import java.sql.ResultSet;

import static io.spine.server.storage.jdbc.message.MessageTable.bytesColumn;
import static io.spine.server.storage.jdbc.query.ColumnReaderFactory.messageReader;

abstract class SelectByShardIndexQuery<M extends Message>
        extends AbstractQuery
        implements SelectQuery<DbIterator<M>> {

    private final long shardIndex;

    SelectByShardIndexQuery(
            Builder<? extends Builder, ? extends SelectByShardIndexQuery> builder) {
        super(builder);
        this.shardIndex = builder.shardIndex;
    }

    @Override
    public DbIterator<M> execute() {
        ResultSet resultSet = query().getResults();
        DbIterator<M> iterator =
                DbIterator.over(resultSet,
                                messageReader(bytesColumn().name(), messageDescriptor()));
        return iterator;
    }

    protected abstract AbstractSQLQuery<Object, ?> query();

    protected abstract Descriptor messageDescriptor();

    protected long shardIndex() {
        return shardIndex;
    }

    abstract static class Builder<B extends Builder<B, Q>, Q extends SelectByShardIndexQuery>
            extends AbstractQuery.Builder<B, Q> {

        private long shardIndex;

        B setShardIndex(long shardIndex) {
            this.shardIndex = shardIndex;
            return getThis();
        }
    }
}
