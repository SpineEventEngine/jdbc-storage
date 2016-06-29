package org.spine3.server.storage.jdbc.query.tables.commands;

import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.AbstractQuery;
import org.spine3.server.storage.jdbc.query.Write;
import org.spine3.server.storage.jdbc.query.constants.CommandTable;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CreateTableIfDoesNotExistQuery extends AbstractQuery implements Write {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String INSERT_QUERY =
            "CREATE TABLE IF NOT EXISTS " + CommandTable.TABLE_NAME + " (" +
                    CommandTable.ID_COL + " VARCHAR(512), " +
                    CommandTable.COMMAND_COL + " BLOB, " +
                    CommandTable.COMMAND_STATUS_COL + " VARCHAR(512), " +
                    CommandTable.ERROR_COL + " BLOB, " +
                    CommandTable.FAILURE_COL + " BLOB, " +
                    " PRIMARY KEY(" + CommandTable.ID_COL + ')' +
                    ");";

    public CreateTableIfDoesNotExistQuery(Builder builder) {
        super(builder);
    }

    public static Builder getBuilder() {
        final Builder builder = new Builder();
        builder.setQuery(INSERT_QUERY);
        return builder;
    }

    @Override
    public void execute() throws DatabaseException {
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             PreparedStatement statement = this.prepareStatement(connection)) {
            statement.execute();
        } catch (SQLException e) {
            //log().error("Exception during table creation:", e);
            throw new DatabaseException(e);
        }
    }

    public static class Builder extends AbstractQuery.Builder<Builder, CreateTableIfDoesNotExistQuery> {

        @Override
        public CreateTableIfDoesNotExistQuery build() {
            return new CreateTableIfDoesNotExistQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }

}