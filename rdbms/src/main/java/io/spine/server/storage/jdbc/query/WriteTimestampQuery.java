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

import com.google.protobuf.Timestamp;
import io.spine.server.storage.jdbc.LastHandledEventTimeTable;
import io.spine.server.storage.jdbc.LastHandledEventTimeTable.Column;

/**
 * A base for the {@linkplain StorageQuery} implementations
 * which write a {@link Timestamp} in the
 * {@link LastHandledEventTimeTable}.
 *
 * @author Dmytro Dashenkov
 */
abstract class WriteTimestampQuery extends WriteQuery {

    private final Timestamp timestamp;
    private final String id;

    WriteTimestampQuery(Builder<? extends Builder, ? extends WriteTimestampQuery> builder) {
        super(builder);
        this.timestamp = builder.timestamp;
        this.id = builder.getId();
    }

    @Override
    protected Parameters getQueryParameters() {
        final Parameter seconds = Parameter.of(timestamp.getSeconds(), Column.seconds);
        final Parameter nanos = Parameter.of(timestamp.getNanos(), Column.nanos);
        final Parameter idParameter = Parameter.of(id, Column.projection_type);
        return Parameters.newBuilder()
                         .addParameter(QueryParameter.SECONDS.getIndex(), seconds)
                         .addParameter(QueryParameter.NANOS.getIndex(), nanos)
                         .addParameter(QueryParameter.PROJECTION_TYPE.getIndex(), idParameter)
                         .build();
    }

    abstract static class Builder<B extends Builder<B, Q>, Q extends WriteTimestampQuery>
            extends WriteQuery.Builder<B, Q> {

        private Timestamp timestamp;
        private String id;

        B setTimestamp(Timestamp timestamp) {
            this.timestamp = timestamp;
            return getThis();
        }

        B setId(String id) {
            this.id = id;
            return getThis();
        }

        String getId() {
            return id;
        }
    }

    private enum QueryParameter {

        SECONDS(1),
        NANOS(2),
        PROJECTION_TYPE(3);

        private final int index;

        QueryParameter(int index) {
            this.index = index;
        }

        private int getIndex() {
            return index;
        }
    }
}
