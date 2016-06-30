/*
 * Copyright 2016, TeamDev Ltd. All rights reserved.
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

package org.spine3.server.storage.jdbc.query.tables.event;

import com.google.protobuf.Timestamp;
import org.spine3.server.storage.EventStorageRecord;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.WriteRecord;
import org.spine3.server.storage.jdbc.query.constants.EventTable;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.spine3.server.storage.jdbc.util.Serializer.serialize;


public class UpdateEventQuery extends WriteRecord<String, EventStorageRecord> {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String UPDATE_QUERY =
            "UPDATE " + EventTable.TABLE_NAME +
                    " SET " +
                    EventTable.EVENT_COL + " = ?, " +
                    EventTable.EVENT_TYPE_COL + " = ?, " +
                    EventTable.PRODUCER_ID_COL + " = ?, " +
                    EventTable.SECONDS_COL + " = ?, " +
                    EventTable.NANOSECONDS_COL + " = ? " +
                    " WHERE " + EventTable.EVENT_ID_COL + " = ? ;";

    private UpdateEventQuery(Builder builder) {
        super(builder);
    }

    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = connection.prepareStatement(UPDATE_QUERY);
            final Timestamp timestamp = this.getRecord().getTimestamp();
        try {
            final byte[] serializedRecord = serialize(this.getRecord());
            statement.setBytes(1, serializedRecord);

            final String eventType = this.getRecord().getEventType();
            statement.setString(2, eventType);

            final String producerId = this.getRecord().getProducerId();
            statement.setString(3, producerId);

            final long seconds = timestamp.getSeconds();
            statement.setLong(4, seconds);

            final int nanos = timestamp.getNanos();
            statement.setInt(5, nanos);

            final String eventId = this.getRecord().getEventId();
            statement.setString(6, eventId);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
        return statement;
    }

    /*protected void logError(SQLException exception) {
        log(exception, "command insertion", getId());
    }*/

    public static Builder getBuilder() {
        final Builder builder = new Builder();
        builder.setQuery(UPDATE_QUERY);
        return builder;
    }

    public static class Builder extends WriteRecord.Builder<Builder, UpdateEventQuery, String, EventStorageRecord> {

        @Override
        public UpdateEventQuery build() {
            return new UpdateEventQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
