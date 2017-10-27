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

import com.google.protobuf.Timestamp;
import com.querydsl.sql.AbstractSQLQuery;
import io.spine.server.storage.jdbc.LastHandledEventTimeTable;
import io.spine.server.storage.jdbc.LastHandledEventTimeTable.Column;

import javax.annotation.Nullable;
import java.sql.ResultSet;
import java.sql.SQLException;

import static io.spine.server.storage.jdbc.LastHandledEventTimeTable.Column.nanos;
import static io.spine.server.storage.jdbc.LastHandledEventTimeTable.Column.seconds;
import static io.spine.validate.Validate.isDefault;

/**
 * Query that selects timestamp from the {@link LastHandledEventTimeTable}.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
class SelectTimestampQuery extends SelectMessageByIdQuery<String, Timestamp> {

    private SelectTimestampQuery(Builder builder) {
        super(builder);
    }

    @Override
    AbstractSQLQuery<?, ?> getQuery() {
        return factory().select(pathOf(seconds), pathOf(nanos))
                        .from(table())
                        .where(hasId());
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod") // Override default Message storing policy
    @Nullable
    @Override
    protected Timestamp readMessage(ResultSet resultSet) throws SQLException {
        final long seconds = resultSet.getLong(Column.seconds.name());
        final int nanos = resultSet.getInt(Column.nanos.name());
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
        SelectTimestampQuery build() {
            return new SelectTimestampQuery(this);
        }

        @Override
        Builder getThis() {
            return this;
        }
    }
}
