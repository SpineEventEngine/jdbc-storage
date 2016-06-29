package org.spine3.server.storage.jdbc.query.tables.commands;

import org.spine3.base.Failure;
import org.spine3.server.storage.jdbc.query.WriteRecord;
import org.spine3.server.storage.jdbc.query.constants.CommandTable;


public class SetFailureQuery extends WriteRecord<String, Failure> {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String SET_FAILURE_QUERY =
            "UPDATE " + CommandTable.TABLE_NAME +
                    " SET " +
                    CommandTable.FAILURE_COL + " = ? " +
                    " WHERE " + CommandTable.ID_COL + " = ? ;";

    private SetFailureQuery(Builder builder) {
        super(builder);
    }

    /*protected void logError(SQLException exception) {
        log(exception, "command insertion", getId());
    }*/

    public static Builder getBuilder() {
        final Builder builder = new Builder();
        builder.setIdIndexInQuery(2)
                .setRecordIndexInQuery(1)
                .setQuery(SET_FAILURE_QUERY);
        return builder;
    }

    public static class Builder extends WriteRecord.Builder<Builder, SetFailureQuery, String, Failure> {

        @Override
        public SetFailureQuery build() {
            return new SetFailureQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
