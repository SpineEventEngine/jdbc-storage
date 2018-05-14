/*
 * Copyright 2018, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.record;

import com.querydsl.core.types.dsl.PathBuilder;
import com.querydsl.sql.dml.SQLInsertClause;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.query.IdColumn;

/**
 * A query that inserts a new {@link EntityRecordWithColumns} into the {@link RecordTable}.
 *
 * @author Dmytro Grankin
 */
class InsertEntityQuery<I> extends WriteEntityQuery<I, SQLInsertClause> {

    private InsertEntityQuery(Builder<I> builder) {
        super(builder);
    }

    @Override
    protected void addBatch(SQLInsertClause clause) {
        clause.addBatch();
    }

    @Override
    protected void setIdValue(SQLInsertClause clause, IdColumn<I> idColumn, Object normalizedId) {
        final PathBuilder<Object> idPath = pathOf(idColumn.getColumnName());
        clause.set(idPath, normalizedId);
    }

    @Override
    protected SQLInsertClause createClause() {
        return factory().insert(table());
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    static class Builder<I> extends WriteEntityQuery.Builder<Builder<I>,
                                                             InsertEntityQuery,
                                                             I> {

        @Override
        protected InsertEntityQuery doBuild() {
            return new InsertEntityQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
