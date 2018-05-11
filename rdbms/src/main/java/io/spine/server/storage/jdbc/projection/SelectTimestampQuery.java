/*
 * Copyright 2017, TeamDev. All rights reserved.
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
import com.querydsl.sql.AbstractSQLQuery;
import io.spine.server.storage.jdbc.projection.LastHandledEventTimeTable.Column;
import io.spine.server.storage.jdbc.query.SelectMessageByIdQuery;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;

import static io.spine.server.storage.jdbc.projection.LastHandledEventTimeTable.Column.NANOS;
import static io.spine.server.storage.jdbc.projection.LastHandledEventTimeTable.Column.SECONDS;
import static io.spine.validate.Validate.isDefault;

/**
 * A query that selects timestamp from the {@link LastHandledEventTimeTable}.
 *
 * @author Dmytro Grankin
 */
class SelectTimestampQuery extends SelectMessageByIdQuery<String, Timestamp> {

    private SelectTimestampQuery(Builder builder) {
        super(builder);
    }

    @Override
    protected AbstractSQLQuery<?, ?> getQuery() {
        final AbstractSQLQuery<?, ?> query = factory().select(pathOf(SECONDS), pathOf(NANOS))
                                                      .from(table())
                                                      .where(hasId());
        return query;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod") // Override default Message storing policy
    @Nullable
    @Override
    protected Timestamp readMessage(ResultSet resultSet) throws SQLException {
        final long seconds = resultSet.getLong(Column.SECONDS.name());
        final int nanos = resultSet.getInt(Column.NANOS.name());
        final Timestamp time = Timestamp.newBuilder()
                                        .setSeconds(seconds)
                                        .setNanos(nanos)
                                        .build();
        if (isDefault(time)) {
            return null;
        }
        return time;
    }

    static Builder newBuilder() {
        return new Builder();
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    static class Builder extends SelectMessageByIdQuery.Builder<Builder,
                                                                SelectTimestampQuery,
                                                                String,
                                                                Timestamp> {
        @Override
        protected SelectTimestampQuery doBuild() {
            return new SelectTimestampQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
