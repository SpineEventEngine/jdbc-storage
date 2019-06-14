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

package io.spine.server.storage.jdbc.aggregate;

import com.querydsl.core.types.Predicate;
import com.querydsl.sql.dml.SQLDeleteClause;
import io.spine.server.storage.jdbc.query.IdAwareQuery;
import io.spine.server.storage.jdbc.query.WriteQuery;

import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.VERSION;

/**
 * A query that deletes records prior to the specified {@code version} in the table.
 *
 * @param <I>
 *         the type of {@code Aggregate} IDs
 */
final class DeletePriorRecordsQuery<I> extends IdAwareQuery<I> implements WriteQuery {

    private final Integer version;

    private DeletePriorRecordsQuery(Builder<I> builder) {
        super(builder);
        this.version = builder.version;
    }

    @Override
    public long execute() {
        SQLDeleteClause query = factory().delete(table())
                                         .where(idEquals())
                                         .where(versionIsPrior());
        return query.execute();
    }

    private Predicate versionIsPrior() {
        return comparablePathOf(VERSION, Integer.class).lt(version);
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I> extends IdAwareQuery.Builder<I, Builder<I>, DeletePriorRecordsQuery<I>> {

        private Integer version;

        Builder<I> setVersion(Integer version) {
            this.version = version;
            return getThis();
        }

        @Override
        protected DeletePriorRecordsQuery<I> doBuild() {
            return new DeletePriorRecordsQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
