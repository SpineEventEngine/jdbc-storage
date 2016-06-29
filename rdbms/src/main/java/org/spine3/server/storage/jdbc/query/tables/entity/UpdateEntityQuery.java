package org.spine3.server.storage.jdbc.query.tables.entity;

import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.jdbc.query.WriteRecord;
import org.spine3.server.storage.jdbc.query.constants.EntityTable;

import static java.lang.String.format;


public class UpdateEntityQuery<Id> extends WriteRecord<Id, EntityStorageRecord> {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String UPDATE_QUERY =
            "UPDATE %s " +
                    " SET " + EntityTable.ENTITY_COL + " = ? " +
                    " WHERE " + EntityTable.ID_COL + " = ?;";

    private UpdateEntityQuery(Builder<Id> builder) {
        super(builder);
    }


    /*protected void logError(SQLException exception) {
        log(exception, "command insertion", getId());
    }*/

    public static <Id> Builder <Id> getBuilder(String tableName) {
        final Builder<Id> builder = new Builder<>();
        builder.setIdIndexInQuery(2)
                .setRecordIndexInQuery(1)
                .setQuery(format(UPDATE_QUERY, tableName));
        return builder;
    }

    public static class Builder<Id> extends WriteRecord.Builder<Builder<Id>, UpdateEntityQuery, Id, EntityStorageRecord> {

        @Override
        public UpdateEntityQuery build() {
            return new UpdateEntityQuery<>(this);
        }

        @Override
        protected Builder<Id> getThis() {
            return this;
        }
    }
}
