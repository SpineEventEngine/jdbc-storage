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

import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.BooleanExpression;
import io.spine.server.storage.jdbc.IdColumn;

/**
 * An abstract base for the queries which read a single record by ID.
 *
 * @author Dmytro Dashenkov
 */
abstract class AbstractSelectByIdQuery<I, R> extends AbstractQuery implements SelectQuery<R> {

    private final IdColumn<I> idColumn;
    private final I id;

    AbstractSelectByIdQuery(Builder<I, ? extends Builder, ? extends StorageQuery> builder) {
        super(builder);
        this.id = builder.getId();
        this.idColumn = builder.getIdColumn();
    }

    /**
     * Obtains a {@link Predicate} for the query.
     *
     * @return a predicate to match records
     */
    Predicate hasId() {
        final String columnName = idColumn.getColumnName();
        final Object normalizedId = idColumn.normalize(id);
        final BooleanExpression hasId = pathOf(columnName).eq(normalizedId);
        return hasId;
    }

    abstract static class Builder<I,
                                  B extends Builder<I, B, Q>,
                                  Q extends AbstractSelectByIdQuery>
            extends AbstractQuery.Builder<B, Q> {

        private IdColumn<I> idColumn;
        private I id;

        B setId(I id) {
            this.id = id;
            return getThis();
        }

        B setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = idColumn;
            return getThis();
        }

        IdColumn<I> getIdColumn() {
            return idColumn;
        }

        I getId() {
            return id;
        }
    }
}
