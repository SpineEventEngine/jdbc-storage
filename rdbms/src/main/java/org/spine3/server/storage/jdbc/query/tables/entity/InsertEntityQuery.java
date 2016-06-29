package org.spine3.server.storage.jdbc.query.tables.entity;

import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.jdbc.query.WriteRecord;
import org.spine3.server.storage.jdbc.query.constants.EntityTable;

import static java.lang.String.format;


public class InsertEntityQuery<Id> extends WriteRecord<Id, EntityStorageRecord> {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String INSERT_QUERY =
            "INSERT INTO %s " +
                    " (" + EntityTable.ID_COL + ", " + EntityTable.ENTITY_COL + ')' +
                    " VALUES (?, ?);";

    private InsertEntityQuery(Builder<Id> builder) {
        super(builder);
    }


    /*protected void logError(SQLException exception) {
        log(exception, "command insertion", getId());
    }*/

    public static <Id> Builder <Id> getBuilder(String tableName) {
        final Builder<Id> builder = new Builder<>();
        builder.setIdIndexInQuery(1)
                .setRecordIndexInQuery(2)
                .setQuery(format(INSERT_QUERY, tableName));
        return builder;
    }

    public static class Builder<Id> extends WriteRecord.Builder<Builder<Id>, InsertEntityQuery, Id, EntityStorageRecord> {

        @Override
        public InsertEntityQuery build() {
            return new InsertEntityQuery<>(this);
        }

        @Override
        protected Builder<Id> getThis() {
            return this;
        }
    }
}
