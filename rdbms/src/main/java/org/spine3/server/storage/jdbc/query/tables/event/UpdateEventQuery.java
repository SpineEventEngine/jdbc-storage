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
