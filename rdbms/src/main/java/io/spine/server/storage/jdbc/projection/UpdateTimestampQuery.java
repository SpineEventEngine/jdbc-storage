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
import com.querydsl.core.types.dsl.PathBuilder;
import com.querydsl.sql.dml.SQLUpdateClause;

import static io.spine.server.storage.jdbc.projection.LastHandledEventTimeTable.Column.nanos;
import static io.spine.server.storage.jdbc.projection.LastHandledEventTimeTable.Column.projection_type;
import static io.spine.server.storage.jdbc.projection.LastHandledEventTimeTable.Column.seconds;

/**
 * A query that updates {@link Timestamp} in the {@link LastHandledEventTimeTable}.
 *
 * @author Dmytro Grankin
 */
class UpdateTimestampQuery extends WriteTimestampQuery {

    private UpdateTimestampQuery(Builder builder) {
        super(builder);
    }

    @Override
    public long execute() {
        final PathBuilder<Object> id = pathOf(projection_type);
        final SQLUpdateClause query = factory().update(table())
                                               .where(id.eq(getIdValue()))
                                               .set(pathOf(seconds), getTimestamp().getSeconds())
                                               .set(pathOf(nanos), getTimestamp().getNanos());
        return query.execute();
    }

    static Builder newBuilder() {
        return new Builder();
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    static class Builder extends WriteTimestampQuery.Builder<Builder, UpdateTimestampQuery> {

        @Override
        protected UpdateTimestampQuery doBuild() {
            return new UpdateTimestampQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
