package org.spine3.server.storage.jdbc.query.tables.commands;

import org.spine3.base.Error;
import org.spine3.base.Failure;
import org.spine3.server.storage.jdbc.query.WriteRecord;
import org.spine3.server.storage.jdbc.query.constants.CommandTable;


public class SetError extends WriteRecord<String, Error > {

    private static final String SET_ERROR_QUERY =
            "UPDATE " + CommandTable.TABLE_NAME +
                    " SET " +
                    CommandTable.ERROR_COL + " = ? " +
                    " WHERE " + CommandTable.ID_COL + " = ? ;";

    private SetError(Builder builder) {
        super(builder);
    }

    /*protected void logError(SQLException exception) {
        log(exception, "command insertion", getId());
    }*/

    public static Builder getBuilder() {
        Builder builder = new Builder();
        builder.setIdIndexInQuery(2)
                .setRecordIndexInQuery(1)
                .setQuery(SET_ERROR_QUERY);
        return builder;
    }

    public static class Builder extends WriteRecord.Builder<Builder, SetError, String, Error> {

        @Override
        public SetError build() {
            return new SetError(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
