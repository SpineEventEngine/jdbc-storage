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
import io.spine.core.Version;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.storage.jdbc.query.IdAwareQuery;
import io.spine.server.storage.jdbc.query.WriteQuery;

import static io.spine.server.storage.jdbc.aggregate.AggregateEventRecordTable.Column.VERSION;

/**
 * A query that deletes the specified {@link AggregateEventRecord aggregate event records} from the
 * table.
 *
 * <p>As records in table can share a common {@code Aggregate} ID, and the record state can in
 * theory be equal to other record's state, the deletion is done by the unique
 * "{@code Aggregate}_ID-to-record_state" combination.
 *
 * @param <I>
 *         the type of {@code Aggregate} IDs
 */
final class DeleteRecordsByVersionQuery<I> extends IdAwareQuery<I> implements WriteQuery {

    private final Version version;

    private DeleteRecordsByVersionQuery(Builder<I> builder) {
        super(builder);
        this.version = builder.version;
    }

    @Override
    public long execute() {
        int versionNumber = version.getNumber();
        Predicate versionIsPrior =
                comparablePathOf(VERSION.name(), Integer.class).lt(versionNumber);
        SQLDeleteClause query = factory().delete(table())
                                         .where(idEquals())
                                         .where(versionIsPrior);
        return query.execute();
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I> extends IdAwareQuery.Builder<I, Builder<I>, DeleteRecordsByVersionQuery<I>> {

        private Version version;

        Builder<I> setVersion(Version version) {
            this.version = version;
            return getThis();
        }

        @Override
        protected DeleteRecordsByVersionQuery<I> doBuild() {
            return new DeleteRecordsByVersionQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
