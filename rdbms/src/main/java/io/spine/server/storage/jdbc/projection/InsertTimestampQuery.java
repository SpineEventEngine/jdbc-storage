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

package io.spine.server.storage.jdbc.projection;

import com.google.protobuf.Timestamp;
import com.querydsl.core.dml.StoreClause;
import io.spine.server.storage.jdbc.query.Parameter;
import io.spine.server.storage.jdbc.query.Parameters;

import static io.spine.server.storage.jdbc.projection.LastHandledEventTimeTable.Column.projection_type;

/**
 * Query that inserts a new {@link Timestamp} to the {@link LastHandledEventTimeTable}.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
class InsertTimestampQuery extends WriteTimestampQuery {

    private InsertTimestampQuery(Builder builder) {
        super(builder);
    }

    @Override
    protected StoreClause<?> createClause() {
        return factory().insert(table());
    }

    @Override
    protected Parameters getParameters() {
        final Parameters superParameters = super.getParameters();
        final Parameter idParameter = Parameter.of(getId());
        return Parameters.newBuilder()
                         .addParameters(superParameters)
                         .addParameter(projection_type.name(), idParameter)
                         .build();
    }

    static Builder newBuilder() {
        return new Builder();
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    static class Builder extends WriteTimestampQuery.Builder<Builder, InsertTimestampQuery> {

        @Override
        public InsertTimestampQuery build() {
            return new InsertTimestampQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
