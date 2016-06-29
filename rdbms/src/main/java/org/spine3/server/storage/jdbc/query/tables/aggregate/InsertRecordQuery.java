package org.spine3.server.storage.jdbc.query.tables.aggregate;

import com.google.protobuf.Timestamp;
import org.spine3.server.storage.AggregateStorageRecord;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.WriteRecord;
import org.spine3.server.storage.jdbc.query.constants.AggregateTable;
import org.spine3.server.storage.jdbc.query.constants.EntityTable;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.Serializer;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static java.lang.String.format;


public class InsertRecordQuery<Id> extends WriteRecord<Id, AggregateStorageRecord> {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String INSERT_QUERY =
            "INSERT INTO %s " +
                    " (" + AggregateTable.ID_COL + ", " + AggregateTable.AGGREGATE_COL + ", " + AggregateTable.SECONDS_COL + ", " + AggregateTable.NANOS_COL + ") " +
                    " VALUES (?, ?, ?, ?);";

    private InsertRecordQuery(Builder<Id> builder) {
        super(builder);
    }


    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);

        try {
            final Timestamp timestamp = this.getRecord().getTimestamp();
            statement.setLong(3, timestamp.getSeconds());
            statement.setInt(4, timestamp.getNanos());
            return statement;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    /*protected void logError(SQLException exception) {
        log(exception, "command insertion", getId());
    }*/

    public static <Id> Builder <Id> getBuilder(String tableName) {
        final Builder<Id> builder = new Builder<>();
        builder.setIdIndexInQuery(1)
                .setRecordIndexInQuery(2)
                .setQuery(format(INSERT_QUERY, tableName));
        return builder;
    }

    public static class Builder<Id> extends WriteRecord.Builder<Builder<Id>, InsertRecordQuery, Id, AggregateStorageRecord> {

        @Override
        public InsertRecordQuery build() {
            return new InsertRecordQuery<>(this);
        }

        @Override
        protected Builder<Id> getThis() {
            return this;
        }
    }
}
