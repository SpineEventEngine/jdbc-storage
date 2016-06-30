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

package org.spine3.server.storage.jdbc.event.query;

import com.google.protobuf.Timestamp;
import org.spine3.server.storage.EventStorageRecord;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.WriteRecord;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.spine3.server.storage.jdbc.util.Serializer.serialize;


public class InsertEventQuery extends WriteRecord<String, EventStorageRecord> {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String INSERT_QUERY =
            "INSERT INTO " + EventTable.TABLE_NAME + " (" +
                    EventTable.EVENT_ID_COL + ", " +
                    EventTable.EVENT_COL + ", " +
                    EventTable.EVENT_TYPE_COL + ", " +
                    EventTable.PRODUCER_ID_COL + ", " +
                    EventTable.SECONDS_COL + ", " +
                    EventTable.NANOSECONDS_COL +
                    ") VALUES (?, ?, ?, ?, ?, ?);";

    private InsertEventQuery(Builder builder) {
        super(builder);
    }

    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = connection.prepareStatement(INSERT_QUERY);
            final Timestamp timestamp = this.getRecord().getTimestamp();
        try {
            final String eventId = this.getRecord().getEventId();
            statement.setString(1, eventId);

            final byte[] serializedRecord = serialize(this.getRecord());
            statement.setBytes(2, serializedRecord);

            final String eventType = this.getRecord().getEventType();
            statement.setString(3, eventType);

            final String producerId = this.getRecord().getProducerId();
            statement.setString(4, producerId);

            final long seconds = timestamp.getSeconds();
            statement.setLong(5, seconds);

            final int nanos = timestamp.getNanos();
            statement.setInt(6, nanos);
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
        builder.setQuery(INSERT_QUERY);
        return builder;
    }

    public static class Builder extends WriteRecord.Builder<Builder, InsertEventQuery, String, EventStorageRecord> {

        @Override
        public InsertEventQuery build() {
            return new InsertEventQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
