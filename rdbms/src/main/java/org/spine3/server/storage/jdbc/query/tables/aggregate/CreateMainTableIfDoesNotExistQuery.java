package org.spine3.server.storage.jdbc.query.tables.aggregate;

import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.AbstractQuery;
import org.spine3.server.storage.jdbc.query.constants.AggregateTable;
import org.spine3.server.storage.jdbc.query.constants.EntityTable;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static java.lang.String.format;

public class CreateMainTableIfDoesNotExistQuery extends AbstractQuery {

    private final IdColumn idColumn;
    private final String tableName;

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String QUERY =
            "CREATE TABLE IF NOT EXISTS %s (" +
                    AggregateTable.ID_COL + " %s, " +
                    AggregateTable.AGGREGATE_COL + " BLOB, " +
                    AggregateTable.SECONDS_COL + " BIGINT, " +
                    AggregateTable.NANOS_COL + " INT " +
                    ");";

    protected CreateMainTableIfDoesNotExistQuery(Builder builder) {
        super(builder);
        this.idColumn = IdColumn.newInstance(builder.idType);
        this.tableName = builder.tableName;
    }

    public void execute() throws DatabaseException {
        final String idColumnType = idColumn.getColumnDataType();
        final String createTableSql = format(QUERY, tableName, idColumnType);
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             PreparedStatement statement = connection.prepareStatement(createTableSql)) {
            statement.execute();
        } catch (SQLException e) {
            //log().error("Error while creating a table with the name: " + tableName, e);
            throw new DatabaseException(e);
        }
    }

    public static Builder getBuilder() {
        final Builder builder = new Builder();
        builder.setQuery(QUERY);
        return builder;
    }

    public static class Builder extends AbstractQuery.Builder<Builder, CreateMainTableIfDoesNotExistQuery> {

        private String idType;
        private String tableName;

        @Override
        public CreateMainTableIfDoesNotExistQuery build() {
            return new CreateMainTableIfDoesNotExistQuery(this);
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