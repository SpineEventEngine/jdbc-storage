package org.spine3.server.storage.jdbc.query.tables.aggregate;

import com.google.protobuf.Timestamp;
import org.spine3.base.CommandStatus;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.UpdateRecord;
import org.spine3.server.storage.jdbc.query.constants.AggregateTable;
import org.spine3.server.storage.jdbc.query.constants.CommandTable;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static java.lang.String.format;


public class InsertEventCountQuery<Id> extends UpdateRecord<Id> {

    private final int count;

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String INSERT_QUERY =
            "INSERT INTO %s " +
                    " (" + AggregateTable.ID_COL + ", " + AggregateTable.EVENT_COUNT_COL + ')' +
                    " VALUES (?, ?);";

    private InsertEventCountQuery(Builder<Id> builder) {
        super(builder);
        this.count = builder.count;
    }

    /*protected void logError(SQLException exception) {
        log(exception, "command insertion", getId());
    }*/

    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);

        try {
            statement.setInt(2, count);
            return statement;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public static <Id> Builder<Id> getBuilder(String tableName) {
        final Builder<Id> builder = new Builder<Id>();
        builder.setQuery(format(INSERT_QUERY, tableName))
                .setIdIndexInQuery(1);
        return builder;
    }

    public static class Builder<Id> extends UpdateRecord.Builder<Builder<Id>, InsertEventCountQuery, Id> {

        private int count;

        @Override
        public InsertEventCountQuery<Id> build() {
            return new InsertEventCountQuery<>(this);
        }

        public Builder<Id> setCount(int count){
            this.count = count;
            return getThis();
        }

        @Override
        protected Builder<Id> getThis() {
            return this;
        }
    }
}
