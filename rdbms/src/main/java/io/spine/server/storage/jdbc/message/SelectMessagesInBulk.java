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

package io.spine.server.storage.jdbc.message;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.querydsl.sql.AbstractSQLQuery;
import io.spine.server.storage.jdbc.query.AbstractQuery;
import io.spine.server.storage.jdbc.query.DbIterator;
import io.spine.server.storage.jdbc.query.IdColumn;
import io.spine.server.storage.jdbc.query.SelectQuery;

import java.sql.ResultSet;
import java.util.List;

import static io.spine.server.storage.jdbc.message.MessageTable.bytesColumn;
import static io.spine.server.storage.jdbc.query.ColumnReaderFactory.messageReader;
import static java.util.stream.Collectors.toList;

final class SelectMessagesInBulk<I, M extends Message>
        extends AbstractQuery
        implements SelectQuery<DbIterator<M>> {

    private final ImmutableList<I> ids;
    private final IdColumn<I> idColumn;
    private final Descriptor messageDescriptor;

    private SelectMessagesInBulk(Builder<I, M> builder) {
        super(builder);
        this.ids = builder.ids;
        this.idColumn = builder.idColumn;
        this.messageDescriptor = builder.messageDescriptor;
    }

    @Override
    public DbIterator<M> execute() {
        ResultSet results = query().getResults();
        DbIterator<M> iterator =
                DbIterator.over(results,
                                messageReader(bytesColumn().name(), messageDescriptor));
        return iterator;
    }

    private AbstractSQLQuery<Object, ?> query() {
        List<Object> normalizedIds = ids
                .stream()
                .map(idColumn::normalize)
                .collect(toList());
        return factory().select(pathOf(bytesColumn()))
                        .from(table())
                        .where(pathOf(idColumn).in(normalizedIds));
    }

    static <I, M extends Message> Builder<I, M> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I, M extends Message>
            extends AbstractQuery.Builder<Builder<I, M>, SelectMessagesInBulk<I, M>> {

        private ImmutableList<I> ids;
        private IdColumn<I> idColumn;
        private Descriptor messageDescriptor;

        Builder<I, M> setIds(Iterable<I> ids) {
            this.ids = ImmutableList.copyOf(ids);
            return getThis();
        }

        Builder<I, M> setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = idColumn;
            return getThis();
        }

        Builder<I, M> setMessageDescriptor(Descriptor messageDescriptor) {
            this.messageDescriptor = messageDescriptor;
            return getThis();
        }

        @Override
        protected Builder<I, M> getThis() {
            return this;
        }

        @Override
        protected SelectMessagesInBulk<I, M> doBuild() {
            return new SelectMessagesInBulk<>(this);
        }
    }
}
