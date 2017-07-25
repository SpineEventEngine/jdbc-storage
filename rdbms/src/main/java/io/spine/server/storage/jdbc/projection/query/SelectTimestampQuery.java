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
import io.spine.server.storage.jdbc.query.SelectMessageByIdQuery;
import io.spine.server.storage.jdbc.table.LastHandledEventTimeTable;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.lang.String.format;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static io.spine.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static io.spine.server.storage.jdbc.Sql.Query.FROM;
import static io.spine.server.storage.jdbc.Sql.Query.PLACEHOLDER;
import static io.spine.server.storage.jdbc.Sql.Query.SELECT;
import static io.spine.server.storage.jdbc.Sql.Query.WHERE;
import static io.spine.server.storage.jdbc.table.LastHandledEventTimeTable.Column.nanos;
import static io.spine.server.storage.jdbc.table.LastHandledEventTimeTable.Column.projection_type;
import static io.spine.server.storage.jdbc.table.LastHandledEventTimeTable.Column.seconds;
import static io.spine.validate.Validate.isDefault;

/**
 * Query that selects timestamp from the
 * {@link LastHandledEventTimeTable}.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class SelectTimestampQuery extends SelectMessageByIdQuery<String, Timestamp> {

    private static final String QUERY_TEMPLATE = SELECT.toString() +
                                                 seconds + COMMA +
                                                 nanos + FROM + "%s" +
                                                 WHERE + projection_type + EQUAL + PLACEHOLDER +
                                                 SEMICOLON;

    private SelectTimestampQuery(Builder builder) {
        super(builder);
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod") // Override default Message storing policy
    @Nullable
    @Override
    protected Timestamp readMessage(ResultSet resultSet) throws SQLException {
        final long seconds = resultSet.getLong(LastHandledEventTimeTable.Column.seconds.toString());
        final int nanos = resultSet.getInt(LastHandledEventTimeTable.Column.nanos.toString());
        final Timestamp time = Timestamp.newBuilder()
                                        .setSeconds(seconds)
                                        .setNanos(nanos)
                                        .build();
        if (isDefault(time)) {
            return null;
        }
        return time;
    }

    public static Builder newBuilder(String tableName) {
        final Builder builder = new Builder();
        builder.setQuery(format(QUERY_TEMPLATE, tableName))
               .setIdIndexInQuery(1);
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder extends SelectMessageByIdQuery.Builder<Builder,
                                                                       SelectTimestampQuery,
                                                                       String,
                                                                       Timestamp> {
        @Override
        public SelectTimestampQuery build() {
            return new SelectTimestampQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
