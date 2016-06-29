package org.spine3.server.storage.jdbc.query.tables.projection;

import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.AbstractQuery;
import org.spine3.server.storage.jdbc.query.constants.ProjectionTable;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import com.google.protobuf.Timestamp;

import static java.lang.String.format;


public class InsertTimestampQuery<Id> extends AbstractQuery{

    private final Timestamp timestamp;

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String INSERT_QUERY =
            "INSERT INTO %s " +
                    " (" + ProjectionTable.SECONDS_COL + ", " + ProjectionTable.NANOS_COL + ')' +
                    " VALUES (?, ?);";

    private InsertTimestampQuery(Builder<Id> builder) {
        super(builder);
        this.timestamp = builder.timestamp;
    }


    /*protected void logError(SQLException exception) {
        log(exception, "command insertion", getId());
    }*/

    public void execute() {
        try (ConnectionWrapper connection = dataSource.getConnection(false)) {
            try (PreparedStatement statement = prepareStatement(connection)) {
                statement.execute();
                connection.commit();
            } catch (SQLException e) {
                //logError(e);
                connection.rollback();
                throw new DatabaseException(e);
            }
        }
    }

    public static <Id> Builder <Id> getBuilder(String tableName) {
        final Builder<Id> builder = new Builder<>();
        builder.setQuery(format(INSERT_QUERY, tableName));
        return builder;
    }

    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);
        final long seconds = timestamp.getSeconds();
        final int nanos = timestamp.getNanos();
        try {
            statement.setLong(1, seconds);
            statement.setInt(2, nanos);
            return statement;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public static class Builder<Id> extends AbstractQuery.Builder<Builder<Id>, InsertTimestampQuery> {

        private Timestamp timestamp;

        @Override
        public InsertTimestampQuery build() {
            return new InsertTimestampQuery<>(this);
        }

        public Builder<Id> setTimestamp(Timestamp timestamp){
            this.timestamp = timestamp;
            return getThis();
        }

        @Override
        protected Builder<Id> getThis() {
            return this;
        }
    }
}
