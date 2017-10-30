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

package io.spine.server.storage.jdbc.query.dsl;

import com.querydsl.core.dml.StoreClause;
import io.spine.server.storage.jdbc.query.Parameters;
import io.spine.server.storage.jdbc.query.WriteQuery;

import java.util.Set;

/**
 * An abstract base for {@code INSERT} and {@code UPDATE} queries.
 *
 * @author Alexander Litus
 */
abstract class AbstractStoreQuery extends AbstractQuery implements WriteQuery {

    AbstractStoreQuery(Builder<? extends Builder, ? extends WriteQuery> builder) {
        super(builder);
    }

    @Override
    public long execute() {
        final StoreClause<?> clause = createClause();
        setParameters(clause, getParameters());
        return clause.execute();
    }

    /**
     * Creates an {@link StoreClause clause} representing the query.
     *
     * @return a new {@link StoreClause} instance
     */
    abstract StoreClause<?> createClause();

    /**
     * Obtains parameters to set for the query.
     *
     * @return the query parameters
     */
    abstract Parameters getParameters();

    private void setParameters(StoreClause<?> clause, Parameters parameters) {
        final Set<String> identifiers = parameters.getIdentifiers();
        for (String identifier : identifiers) {
            final Object parameterValue = parameters.getParameter(identifier)
                                                    .getValue();
            clause.set(pathOf(identifier), parameterValue);
        }
    }

    abstract static class Builder<B extends Builder<B, Q>, Q extends AbstractStoreQuery>
            extends AbstractQuery.Builder<B, Q> {
    }
}
