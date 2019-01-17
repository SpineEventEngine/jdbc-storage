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

import com.querydsl.sql.dml.SQLInsertClause;

import static io.spine.server.storage.jdbc.aggregate.EventCountTable.Column.EVENT_COUNT;

/**
 * A query that inserts a new aggregate event count after the last snapshot into the
 * {@link EventCountTable}.
 *
 * @author Dmytro Grankin
 */
class InsertEventCountQuery<I> extends WriteEventCountQuery<I> {

    private InsertEventCountQuery(Builder<I> builder) {
        super(builder);
    }

    @Override
    public long execute() {
        SQLInsertClause query = factory().insert(table())
                                         .set(idPath(), getNormalizedId())
                                         .set(pathOf(EVENT_COUNT), getEventCount());
        return query.execute();
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    static class Builder<I> extends WriteEventCountQuery.Builder<Builder<I>,
                                                                 InsertEventCountQuery<I>,
                                                                 I> {

        @Override
        protected InsertEventCountQuery<I> doBuild() {
            return new InsertEventCountQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
