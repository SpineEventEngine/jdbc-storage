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

package org.spine3.server.storage.jdbc.projection.query;

import com.google.protobuf.Timestamp;

import static java.lang.String.format;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.BRACKET_CLOSE;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.BRACKET_OPEN;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.INSERT_INTO;
import static org.spine3.server.storage.jdbc.Sql.Query.VALUES;
import static org.spine3.server.storage.jdbc.Sql.nPlaceholders;
import static org.spine3.server.storage.jdbc.table.LastHandledEventTimeTable.Column.nanos;
import static org.spine3.server.storage.jdbc.table.LastHandledEventTimeTable.Column.projection_type;
import static org.spine3.server.storage.jdbc.table.LastHandledEventTimeTable.Column.seconds;

/**
 * Query that inserts a new {@link Timestamp} to the
 * {@link org.spine3.server.storage.jdbc.table.LastHandledEventTimeTable}.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class InsertTimestampQuery extends WriteTimestampQuery {

    private static final String QUERY_TEMPLATE =
            INSERT_INTO + "%s" +
            BRACKET_OPEN + seconds + COMMA + nanos + COMMA + projection_type + BRACKET_CLOSE +
            VALUES + nPlaceholders(3) + SEMICOLON;

    private InsertTimestampQuery(Builder builder) {
        super(builder);
    }

    public static Builder newBuilder(String tableName) {
        final Builder builder = new Builder();
        builder.setQuery(format(QUERY_TEMPLATE, tableName));
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder extends WriteTimestampQuery.Builder<Builder, InsertTimestampQuery> {

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
