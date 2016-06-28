package org.spine3.server.storage.jdbc.query.tables.entity;

import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.jdbc.query.WriteRecord;
import org.spine3.server.storage.jdbc.query.constants.CommandTable;
import org.spine3.server.storage.jdbc.query.constants.EntityTable;
import org.spine3.server.storage.jdbc.query.tables.commands.WriteCommandRecord;
import org.spine3.server.storage.jdbc.util.IdColumn;


public class InsertEntityQuery extends WriteRecord {

    private final IdColumn idColumn;
    private final String tableName;

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String INSERT_QUERY =
            "INSERT INTO %s " +
                    " (" + EntityTable.ID_COL + ", " + EntityTable.ENTITY_COL + ')' +
                    " VALUES (?, ?);";

    private InsertEntityQuery(Builder builder) {
        super(builder);
        this.idColumn = IdColumn.newInstance(builder.idType);
        this.tableName = builder.tableName;
    }

    /*protected void logError(SQLException exception) {
        log(exception, "command insertion", getId());
    }*/

    public static Builder getBuilder() {
        Builder builder = new Builder();
        builder
                .setIdIndexInQuery(1)
                .setRecordIndexInQuery(3)
                .setQuery(INSERT_QUERY);
        return builder;
    }

    public static class Builder extends WriteRecord.Builder<Builder, InsertEntityQuery, String, EntityStorageRecord> {

        private String idType;
        private String tableName;

        @Override
        public InsertEntityQuery build() {
            return new InsertEntityQuery(this);
        }

        public Builder setIdType(String idType){
            this.idType = idType;
            return getThis();
        }

        public Builder setTableName(String tableName){
            this.tableName = tableName;
            return getThis();
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
