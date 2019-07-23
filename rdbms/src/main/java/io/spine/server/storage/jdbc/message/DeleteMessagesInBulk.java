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
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.querydsl.sql.dml.SQLDeleteClause;
import io.spine.server.storage.jdbc.query.AbstractQuery;
import io.spine.server.storage.jdbc.query.IdColumn;
import io.spine.server.storage.jdbc.query.WriteQuery;

import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * Deletes multiple messages by IDs from the {@link MessageTable}.
 *
 * @param <I>
 *         the ID type
 */
final class DeleteMessagesInBulk<I> extends AbstractQuery implements WriteQuery {

    private final ImmutableList<I> ids;
    private final IdColumn<I> idColumn;

    private DeleteMessagesInBulk(Builder<I> builder) {
        super(builder);
        this.ids = builder.ids;
        this.idColumn = builder.idColumn;
    }

    @CanIgnoreReturnValue
    @Override
    public long execute() {
        List<Object> normalizedIds = ids
                .stream()
                .map(idColumn::normalize)
                .collect(toList());
        SQLDeleteClause query = factory().delete(table())
                                         .where(pathOf(idColumn).in(normalizedIds));
        return query.execute();
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I> extends AbstractQuery.Builder<Builder<I>, DeleteMessagesInBulk<I>> {

        private ImmutableList<I> ids;
        private IdColumn<I> idColumn;

        Builder<I> setIds(Iterable<I> ids) {
            this.ids = ImmutableList.copyOf(ids);
            return getThis();
        }

        Builder<I> setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = idColumn;
            return getThis();
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }

        @Override
        protected DeleteMessagesInBulk<I> doBuild() {
            return new DeleteMessagesInBulk<>(this);
        }
    }
}
