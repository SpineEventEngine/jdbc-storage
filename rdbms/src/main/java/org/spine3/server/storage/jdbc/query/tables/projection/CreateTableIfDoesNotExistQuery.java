package org.spine3.server.storage.jdbc.query.tables.projection;

import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.AbstractQuery;
import org.spine3.server.storage.jdbc.query.constants.EntityTable;
import org.spine3.server.storage.jdbc.query.constants.ProjectionTable;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static java.lang.String.format;

public class CreateTableIfDoesNotExistQuery extends AbstractQuery {

    private final String tableName;

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String CREATE_TABLE_IF_DOES_NOT_EXIST =
            "CREATE TABLE IF NOT EXISTS %s (" +
                    ProjectionTable.SECONDS_COL + " BIGINT, " +
                    ProjectionTable.NANOS_COL + " INT " +
                    ");";

    protected CreateTableIfDoesNotExistQuery(Builder builder) {
        super(builder);
        this.tableName = builder.tableName;
    }

    public void execute() throws DatabaseException {
        final String createTableSql = format(CREATE_TABLE_IF_DOES_NOT_EXIST, tableName);
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
        builder.setQuery(CREATE_TABLE_IF_DOES_NOT_EXIST);
        return builder;
    }

    public static class Builder extends AbstractQuery.Builder<Builder, CreateTableIfDoesNotExistQuery> {

        private String tableName;

        @Override
        public CreateTableIfDoesNotExistQuery build() {
            return new CreateTableIfDoesNotExistQuery(this);
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