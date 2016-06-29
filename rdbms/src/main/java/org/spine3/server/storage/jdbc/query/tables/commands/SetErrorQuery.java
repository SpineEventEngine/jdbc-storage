package org.spine3.server.storage.jdbc.query.tables.commands;

import org.spine3.base.Error;
import org.spine3.server.storage.jdbc.query.WriteRecord;
import org.spine3.server.storage.jdbc.query.constants.CommandTable;


public class SetErrorQuery extends WriteRecord<String, Error > {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String SET_ERROR_QUERY =
            "UPDATE " + CommandTable.TABLE_NAME +
                    " SET " +
                    CommandTable.ERROR_COL + " = ? " +
                    " WHERE " + CommandTable.ID_COL + " = ? ;";

    private SetErrorQuery(Builder builder) {
        super(builder);
    }

    /*protected void logError(SQLException exception) {
        log(exception, "command insertion", getId());
    }*/

    public static Builder getBuilder() {
        final Builder builder = new Builder();
        builder.setIdIndexInQuery(2)
                .setRecordIndexInQuery(1)
                .setQuery(SET_ERROR_QUERY);
        return builder;
    }

    public static class Builder extends WriteRecord.Builder<Builder, SetErrorQuery, String, Error> {

        @Override
        public SetErrorQuery build() {
            return new SetErrorQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
