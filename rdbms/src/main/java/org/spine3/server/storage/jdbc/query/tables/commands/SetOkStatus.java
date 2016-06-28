package org.spine3.server.storage.jdbc.query.tables.commands;

import org.spine3.base.CommandStatus;
import org.spine3.server.storage.jdbc.query.UpdateRecord;
import org.spine3.server.storage.jdbc.query.constants.CommandTable;


public class SetOkStatus extends UpdateRecord<String> {

    private static final String SET_OK_STATUS_QUERY =
            "UPDATE " + CommandTable.TABLE_NAME +
            " SET " + CommandTable.COMMAND_STATUS_COL + " = '" + CommandStatus.forNumber(CommandStatus.OK_VALUE).name() + "'" +
            " WHERE " + CommandTable.ID_COL + " = ? ;";

    private SetOkStatus(Builder builder) {
        super(builder);
    }

    /*protected void logError(SQLException exception) {
        log(exception, "command insertion", getId());
    }*/

    public static Builder getBuilder() {
        Builder builder = new Builder();
        builder.setIdIndexInQuery(1)
               .setQuery(SET_OK_STATUS_QUERY);
        return builder;
    }

    public static class Builder extends UpdateRecord.Builder<Builder, SetOkStatus, String> {

        @Override
        public SetOkStatus build() {
            return new SetOkStatus(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
