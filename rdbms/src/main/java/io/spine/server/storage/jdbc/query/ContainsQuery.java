/*
 * Copyright 2017, TeamDev Ltd. All rights reserved.
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

package io.spine.server.storage.jdbc.query;

import com.querydsl.core.types.dsl.PathBuilder;
import com.querydsl.sql.AbstractSQLQuery;
import io.spine.server.storage.jdbc.IdColumn;

import static com.querydsl.sql.SQLExpressions.count;

/**
 * A query that checks if the table contains a record with given ID.
 *
 * @author Dmytro Dashenkov
 */
class ContainsQuery<I> extends AbstractQuery implements SelectQuery<Boolean> {

    private final IdColumn<I> idColumn;
    private final I id;

    private ContainsQuery(Builder<I> builder) {
        super(builder);
        this.idColumn = builder.idColumn;
        this.id = builder.id;
    }

    /**
     * @return {@code true} if there is at least one record with given ID, {@code} false otherwise
     */
    @Override
    public Boolean execute() {
        final PathBuilder<Object> idPath = pathOf(idColumn.getColumnName());
        final Object normalizedId = idColumn.normalize(id);
        final AbstractSQLQuery<Long, ?> query = factory().select(count())
                                                         .from(table())
                                                         .where(idPath.eq(normalizedId));
        final long recordsCount = query.fetchOne();
        return recordsCount > 0;
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I> extends AbstractQuery.Builder<Builder<I>, ContainsQuery<I>> {

        private IdColumn<I> idColumn;
        private I id;

        Builder<I> setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = idColumn;
            return this;
        }

        Builder<I> setId(I id) {
            this.id = id;
            return this;
        }

        @Override
        ContainsQuery<I> build() {
            return new ContainsQuery<>(this);
        }

        @Override
        Builder<I> getThis() {
            return this;
        }
    }
}
