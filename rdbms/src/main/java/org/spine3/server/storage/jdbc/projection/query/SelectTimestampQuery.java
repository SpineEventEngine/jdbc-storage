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
import org.spine3.server.storage.jdbc.Sql;
import org.spine3.server.storage.jdbc.query.SelectByIdQuery;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.lang.String.format;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static org.spine3.server.storage.jdbc.Sql.Query.FROM;
import static org.spine3.server.storage.jdbc.Sql.Query.SELECT;
import static org.spine3.server.storage.jdbc.projection.query.ProjectionTable.NANOS_COL;
import static org.spine3.server.storage.jdbc.projection.query.ProjectionTable.SECONDS_COL;
import static org.spine3.validate.Validate.isDefault;

/**
 * Query that selects timestamp from the {@link ProjectionTable}.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class SelectTimestampQuery extends SelectByIdQuery<String, Timestamp> {

    private static final String QUERY_TEMPLATE = SELECT +
                                                 SECONDS_COL + COMMA +
                                                 NANOS_COL + FROM + "%s" +
                                                 Sql.BuildingBlock.SEMICOLON;

    private SelectTimestampQuery(Builder builder) {
        super(builder);
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod") // Override default Message storing policy
    @Nullable
    @Override
    protected Timestamp readMessage(ResultSet resultSet) throws SQLException {
        final long seconds = resultSet.getLong(SECONDS_COL);
        final int nanos = resultSet.getInt(NANOS_COL);
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
    public static class Builder extends SelectByIdQuery.Builder<Builder,
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
