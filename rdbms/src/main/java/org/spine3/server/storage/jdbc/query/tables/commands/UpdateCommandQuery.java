package org.spine3.server.storage.jdbc.query.tables.commands;

import org.spine3.server.storage.jdbc.query.constants.CommandTable;


public class UpdateCommandQuery extends WriteCommandRecordQuery {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String UPDATE_QUERY =
            "UPDATE " + CommandTable.TABLE_NAME +
                    " SET " + CommandTable.COMMAND_COL + " = ? " +
                    ", " + CommandTable.COMMAND_STATUS_COL + " = ? " +
                    " WHERE " + CommandTable.ID_COL + " = ?;";

    private UpdateCommandQuery(Builder builder) {
        super(builder);
    }

    /*protected void logError(SQLException exception) {
        log(exception, "command insertion", getId());
    }*/

    public static Builder getBuilder() {
        final Builder builder = new Builder();
        builder.setStatusIndexInQuery(2)
                .setIdIndexInQuery(3)
                .setRecordIndexInQuery(1)
                .setQuery(UPDATE_QUERY);
        return builder;
    }

    public static class Builder extends WriteCommandRecordQuery.Builder<Builder, UpdateCommandQuery> {

        @Override
        public UpdateCommandQuery build() {
            return new UpdateCommandQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
