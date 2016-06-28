package org.spine3.server.storage.jdbc.query.tables.commands;

import org.spine3.server.storage.jdbc.query.constants.CommandTable;


public class UpdateCommand extends WriteCommandRecord {

    private static final String UPDATE_QUERY =
            "UPDATE " + CommandTable.TABLE_NAME +
                    " SET " + CommandTable.COMMAND_COL + " = ? " +
                    ", " + CommandTable.COMMAND_STATUS_COL + " = ? " +
                    " WHERE " + CommandTable.ID_COL + " = ?;";

    private UpdateCommand(Builder builder) {
        super(builder);
    }

    /*protected void logError(SQLException exception) {
        log(exception, "command insertion", getId());
    }*/

    public static Builder getBuilder() {
        Builder builder = new Builder();
        builder.setStatusIndexInQuery(2)
                .setIdIndexInQuery(3)
                .setRecordIndexInQuery(1)
                .setQuery(UPDATE_QUERY);
        return builder;
    }

    public static class Builder extends WriteCommandRecord.Builder<Builder, UpdateCommand> {

        @Override
        public UpdateCommand build() {
            return new UpdateCommand(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
