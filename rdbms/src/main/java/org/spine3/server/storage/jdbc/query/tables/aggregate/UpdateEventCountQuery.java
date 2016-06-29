package org.spine3.server.storage.jdbc.query.tables.aggregate;

import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.UpdateRecord;
import org.spine3.server.storage.jdbc.query.constants.AggregateTable;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static java.lang.String.format;


public class UpdateEventCountQuery<Id> extends UpdateRecord<Id> {

    private final int count;

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String UPDATE_QUERY =
            "UPDATE %s " +
                    " SET " + AggregateTable.EVENT_COUNT_COL + " = ? " +
                    " WHERE " + AggregateTable.ID_COL + " = ?;";

    private UpdateEventCountQuery(Builder<Id> builder) {
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
            statement.setInt(1, count);
            return statement;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public static <Id> Builder<Id> getBuilder(String tableName) {
        final Builder<Id> builder = new Builder<Id>();
        builder.setQuery(format(UPDATE_QUERY, tableName))
                .setIdIndexInQuery(2);
        return builder;
    }

    public static class Builder<Id> extends UpdateRecord.Builder<Builder<Id>, UpdateEventCountQuery, Id> {

        private int count;

        @Override
        public UpdateEventCountQuery<Id> build() {
            return new UpdateEventCountQuery<>(this);
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
