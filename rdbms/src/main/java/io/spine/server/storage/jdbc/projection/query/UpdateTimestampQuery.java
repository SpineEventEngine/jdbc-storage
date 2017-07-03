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

package io.spine.server.storage.jdbc.projection.query;

import com.google.protobuf.Timestamp;
import io.spine.server.storage.jdbc.Sql;
import io.spine.server.storage.jdbc.table.LastHandledEventTimeTable;

import static java.lang.String.format;

/**
 * Query that updates {@link Timestamp} in the {@link LastHandledEventTimeQueryFactory}.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class UpdateTimestampQuery extends WriteTimestampQuery {

    private static final String QUERY_TEMPLATE =
            Sql.Query.UPDATE + "%s" + Sql.Query.SET +
            LastHandledEventTimeTable.Column.seconds + Sql.BuildingBlock.EQUAL + Sql.Query.PLACEHOLDER + Sql.BuildingBlock.COMMA +
            LastHandledEventTimeTable.Column.nanos + Sql.BuildingBlock.EQUAL + Sql.Query.PLACEHOLDER +
            Sql.Query.WHERE + LastHandledEventTimeTable.Column.projection_type + Sql.BuildingBlock.EQUAL + Sql.Query.PLACEHOLDER + Sql.BuildingBlock.SEMICOLON;

    private UpdateTimestampQuery(Builder builder) {
        super(builder);
    }

    public static Builder newBuilder(String tableName) {
        final Builder builder = new Builder();
        builder.setQuery(format(QUERY_TEMPLATE, tableName));
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder extends WriteTimestampQuery.Builder<Builder, UpdateTimestampQuery> {

        @Override
        public UpdateTimestampQuery build() {
            return new UpdateTimestampQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}