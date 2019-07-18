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
import com.google.protobuf.Message;
import com.querydsl.sql.dml.SQLInsertClause;
import io.spine.server.storage.jdbc.message.MessageTable.Column;
import io.spine.server.storage.jdbc.query.IdAwareQuery;
import io.spine.server.storage.jdbc.query.WriteQuery;

import static com.google.common.base.Preconditions.checkNotNull;

final class InsertMessageQuery<I, M extends Message>
        extends IdAwareQuery<I> implements WriteQuery {

    private final M message;
    private final ImmutableList<? extends Column<M>> columns;

    private InsertMessageQuery(Builder<I, M> builder) {
        super(builder);
        this.message = checkNotNull(builder.message);
        this.columns = checkNotNull(builder.columns);
    }

    @Override
    public long execute() {
        SQLInsertClause query = insertWithId();
        columns.forEach(column -> query.set(pathOf(column), column.getter()
                                                                  .apply(message))
        );
        return query.execute();
    }

    static <I, M extends Message> Builder<I, M> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I, M extends Message>
            extends IdAwareQuery.Builder<I, Builder<I, M>, InsertMessageQuery<I, M>> {

        private M message;
        private ImmutableList<? extends Column<M>> columns;

        Builder<I, M> setMessage(M message) {
            this.message = message;
            return getThis();
        }

        Builder<I, M> setColumns(Iterable<? extends Column<M>> columns) {
            this.columns = ImmutableList.copyOf(columns);
            return getThis();
        }

        @Override
        protected Builder<I, M> getThis() {
            return this;
        }

        @Override
        protected InsertMessageQuery<I, M> doBuild() {
            return new InsertMessageQuery<>(this);
        }
    }
}
