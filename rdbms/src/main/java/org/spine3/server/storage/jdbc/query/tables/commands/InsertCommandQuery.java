package org.spine3.server.storage.jdbc.query.tables.commands;

import org.spine3.server.storage.jdbc.query.constants.CommandTable;

import java.sql.SQLException;


public class InsertCommandQuery extends WriteCommandRecord {

    private static final String INSERT_QUERY =
            "INSERT INTO " + CommandTable.TABLE_NAME + " (" +
                    CommandTable.ID_COL + ", " +
                    CommandTable.COMMAND_STATUS_COL + ", " +
                    CommandTable.COMMAND_COL +
                    ") VALUES (?, ?, ?);";

    private InsertCommandQuery(Builder builder) {
        super(builder);
    }

    /*protected void logError(SQLException exception) {
        log(exception, "command insertion", getId());
    }*/

    public static Builder getBuilder() {
        Builder builder = new Builder();
        builder.setStatusIndexInQuery(2)
                .setIdIndexInQuery(1)
                .setRecordIndexInQuery(3)
                .setQuery(INSERT_QUERY);
        return builder;
    }

    public static class Builder extends WriteCommandRecord.Builder<Builder, InsertCommandQuery> {

        @Override
        public InsertCommandQuery build() {
            return new InsertCommandQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
