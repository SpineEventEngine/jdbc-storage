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
