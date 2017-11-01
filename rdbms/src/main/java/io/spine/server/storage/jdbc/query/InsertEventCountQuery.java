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

import com.querydsl.core.dml.StoreClause;
import io.spine.server.storage.jdbc.aggregate.EventCountTable;

/**
 * A query that inserts a new aggregate event count after the last snapshot to the
 * {@link EventCountTable}.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
class InsertEventCountQuery<I> extends WriteEventCountQuery<I> {

    private InsertEventCountQuery(Builder<I> builder) {
        super(builder);
    }

    @Override
    protected StoreClause<?> createClause() {
        return factory().insert(table());
    }

    @Override
    protected Parameters getParameters() {
        final Parameters superParameters = super.getParameters();
        final Object normalizedId = getIdColumn().normalize(getId());
        final Parameter id = Parameter.of(normalizedId);
        final Parameters result = Parameters.newBuilder()
                                            .addParameters(superParameters)
                                            .addParameter(getIdColumn().getColumnName(), id)
                                            .build();
        return result;
    }

    static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    static class Builder<I> extends WriteEventCountQuery.Builder<Builder<I>,
                                                                 InsertEventCountQuery,
                                                                 I> {

        @Override
        protected InsertEventCountQuery<I> build() {
            return new InsertEventCountQuery<>(this);
        }

        @Override
        protected Builder<I> getThis() {
            return this;
        }
    }
}
