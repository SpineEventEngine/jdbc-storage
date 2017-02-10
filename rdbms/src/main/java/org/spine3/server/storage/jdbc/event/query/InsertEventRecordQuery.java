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

package org.spine3.server.storage.jdbc.event.query;

import com.google.protobuf.Timestamp;
import org.spine3.server.event.storage.EventStorageRecord;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.Sql;
import org.spine3.server.storage.jdbc.query.WriteRecordQuery;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.*;
import static org.spine3.server.storage.jdbc.Sql.Query.INSERT_INTO;
import static org.spine3.server.storage.jdbc.Sql.Query.VALUES;
import static org.spine3.server.storage.jdbc.event.query.EventTable.*;
import static org.spine3.server.storage.jdbc.util.Serializer.serialize;

/**
 * Query that inserts a new {@link EventStorageRecord} to the {@link EventTable}.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class InsertEventRecordQuery extends WriteRecordQuery<String, EventStorageRecord> {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String QUERY_TEMPLATE =
            INSERT_INTO + TABLE_NAME + BRACKET_OPEN +
                    EVENT_ID_COL + COMMA +
                    EVENT_COL + COMMA +
                    EVENT_TYPE_COL + COMMA +
                    PRODUCER_ID_COL + COMMA +
                    SECONDS_COL + COMMA +
                    NANOSECONDS_COL + BRACKET_CLOSE +
                    VALUES + Sql.nPlaceholders(6) + SEMICOLON;

    private InsertEventRecordQuery(Builder builder) {
        super(builder);
    }

    @Override
    @SuppressWarnings("DuplicateStringLiteralInspection")
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = connection.prepareStatement(QUERY_TEMPLATE);
        final Timestamp timestamp = getRecord().getTimestamp();
        try {
            final String eventId = getRecord().getEventId();
            statement.setString(1, eventId);

            final byte[] serializedRecord = serialize(getRecord());
            statement.setBytes(2, serializedRecord);

            final String eventType = getRecord().getEventType();
            statement.setString(3, eventType);

            final String producerId = getRecord().getProducerId();
            statement.setString(4, producerId);

            final long seconds = timestamp.getSeconds();
            statement.setLong(5, seconds);

            final int nanos = timestamp.getNanos();
            statement.setInt(6, nanos);
        } catch (SQLException e) {
            getLogger().error("Failed to build statement", e);
            throw new DatabaseException(e);
        }
        return statement;
    }

    public static Builder newBuilder() {
        final Builder builder = new Builder();
        builder.setQuery(QUERY_TEMPLATE);
        return builder;
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public static class Builder extends WriteRecordQuery.Builder<Builder, InsertEventRecordQuery, String, EventStorageRecord> {

        @Override
        public InsertEventRecordQuery build() {
            return new InsertEventRecordQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
