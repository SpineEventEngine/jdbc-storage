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
import org.spine3.server.storage.jdbc.query.WriteRecordQuery;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.COMMA;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.EQUAL;
import static org.spine3.server.storage.jdbc.Sql.BuildingBlock.SEMICOLON;
import static org.spine3.server.storage.jdbc.Sql.Query.PLACEHOLDER;
import static org.spine3.server.storage.jdbc.Sql.Query.SET;
import static org.spine3.server.storage.jdbc.Sql.Query.UPDATE;
import static org.spine3.server.storage.jdbc.Sql.Query.WHERE;
import static org.spine3.server.storage.jdbc.event.query.EventTable.EVENT_COL;
import static org.spine3.server.storage.jdbc.event.query.EventTable.EVENT_ID_COL;
import static org.spine3.server.storage.jdbc.event.query.EventTable.EVENT_TYPE_COL;
import static org.spine3.server.storage.jdbc.event.query.EventTable.NANOSECONDS_COL;
import static org.spine3.server.storage.jdbc.event.query.EventTable.PRODUCER_ID_COL;
import static org.spine3.server.storage.jdbc.event.query.EventTable.SECONDS_COL;
import static org.spine3.server.storage.jdbc.event.query.EventTable.TABLE_NAME;
import static org.spine3.server.storage.jdbc.util.Serializer.serialize;

/**
 * Query that updates {@link EventStorageRecord} in the {@link EventTable}.
 *
 * @author Alexander Litus
 * @author Andrey Lavrov
 */
public class UpdateEventRecordQuery extends WriteRecordQuery<String, EventStorageRecord> {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String QUERY_TEMPLATE =
            UPDATE + TABLE_NAME +
            SET +
            EVENT_COL + EQUAL + PLACEHOLDER + COMMA +
            EVENT_TYPE_COL + EQUAL + PLACEHOLDER + COMMA +
            PRODUCER_ID_COL + EQUAL + PLACEHOLDER + COMMA +
            SECONDS_COL + EQUAL + PLACEHOLDER + COMMA +
            NANOSECONDS_COL + EQUAL + PLACEHOLDER +
            WHERE + EVENT_ID_COL + EQUAL + PLACEHOLDER + SEMICOLON;

    private UpdateEventRecordQuery(Builder builder) {
        super(builder);
    }

    @Override
    @SuppressWarnings("DuplicateStringLiteralInspection")
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = connection.prepareStatement(QUERY_TEMPLATE);
        final Timestamp timestamp = getRecord()
                .getTimestamp();
        try {
            final byte[] serializedRecord = serialize(getRecord());
            statement.setBytes(1, serializedRecord);

            final String eventType = getRecord()
                    .getEventType();
            statement.setString(2, eventType);

            final String producerId = getRecord()
                    .getProducerId();
            statement.setString(3, producerId);

            final long seconds = timestamp.getSeconds();
            statement.setLong(4, seconds);

            final int nanos = timestamp.getNanos();
            statement.setInt(5, nanos);

            final String eventId = getRecord()
                    .getEventId();
            statement.setString(6, eventId);
        } catch (SQLException e) {
            getLogger().error("Failed to prepare statement ", e);
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
    public static class Builder extends WriteRecordQuery.Builder<Builder, UpdateEventRecordQuery, String, EventStorageRecord> {

        @Override
        public UpdateEventRecordQuery build() {
            return new UpdateEventRecordQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
