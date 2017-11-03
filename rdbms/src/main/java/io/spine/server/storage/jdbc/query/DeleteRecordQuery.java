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

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A query for deleting one or many items by a id of a given column.
 *
 * @author Dmytro Dashenkov
 */
class DeleteRecordQuery<I> extends AbstractQuery implements WriteQuery {

    private final I id;
    private final IdColumn<I> idColumn;

    DeleteRecordQuery(Builder<I> builder) {
        super(builder);
        this.id = builder.columnValue;
        this.idColumn = builder.idColumn;
    }

    @Override
    public long execute() {
        final PathBuilder<Object> idPath = pathOf(idColumn.getColumnName());
        final Object normalizedId = idColumn.normalize(id);
        return factory().delete(table())
                        .where(idPath.eq(normalizedId))
                        .execute();
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I> extends AbstractQuery.Builder<Builder<I>, DeleteRecordQuery> {

        private I columnValue;
        private IdColumn<I> idColumn;

        Builder<I> setIdValue(I value) {
            this.columnValue = checkNotNull(value);
            return getThis();
        }

        Builder<I> setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = idColumn;
            return getThis();
        }

        @Override
        public DeleteRecordQuery<I> build() {
            checkNotNull(idColumn, "ID column must be set");
            checkNotNull(columnValue, "ID must be set");
            return new DeleteRecordQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
