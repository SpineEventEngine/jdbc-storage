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

import com.querydsl.sql.AbstractSQLQuery;

import static com.querydsl.sql.SQLExpressions.count;

/**
 * A query that checks if the table contains a record with the given ID.
 *
 * @author Dmytro Grankin
 */
class ContainsQuery<I> extends IdAwareQuery<I> implements SelectQuery<Boolean> {

    private ContainsQuery(Builder<I> builder) {
        super(builder);
    }

    /**
     * @return {@code true} if there is at least one record with given ID, {@code} false otherwise
     */
    @Override
    public Boolean execute() {
        final AbstractSQLQuery<Long, ?> query = factory().select(count())
                                                         .from(table())
                                                         .where(idPath().eq(getNormalizedId()));
        final long recordsCount = query.fetchOne();
        return recordsCount > 0;
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    static class Builder<I> extends IdAwareQuery.Builder<I, Builder<I>, ContainsQuery<I>> {

        @Override
        protected ContainsQuery<I> doBuild() {
            return new ContainsQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
