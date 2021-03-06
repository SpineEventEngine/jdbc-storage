/*
 * Copyright 2021, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.record;

import com.querydsl.core.types.dsl.PathBuilder;
import com.querydsl.sql.dml.SQLUpdateClause;
import io.spine.server.entity.EntityRecord;
import io.spine.server.storage.jdbc.query.IdColumn;

/**
 * A query that updates {@link EntityRecord} in the {@link RecordTable}.
 */
class UpdateEntityQuery<I> extends WriteEntityQuery<I, SQLUpdateClause> {

    private UpdateEntityQuery(Builder<I> builder) {
        super(builder);
    }

    @Override
    protected void addBatch(SQLUpdateClause clause) {
        clause.addBatch();
    }

    @Override
    protected void setIdValue(SQLUpdateClause clause, IdColumn<I> idColumn, Object normalizedId) {
        PathBuilder<Object> idPath = pathOf(idColumn);
        clause.where(idPath.eq(normalizedId));
    }

    @Override
    protected SQLUpdateClause createClause() {
        return factory().update(table());
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I> extends WriteEntityQuery.Builder<Builder<I>,
                                                             UpdateEntityQuery,
                                                             I> {

        @Override
        protected UpdateEntityQuery doBuild() {
            return new UpdateEntityQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
