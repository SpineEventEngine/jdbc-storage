package org.spine3.server.storage.jdbc.query;

import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.constants.CommandTable;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CreateCommandTableIfNo extends Abstract implements Write {

    private final String query =
            "CREATE TABLE IF NOT EXISTS " + CommandTable.TABLE_NAME + " (" +
                    CommandTable.ID_COL + " VARCHAR(512), " +
                    CommandTable.COMMAND_COL + " BLOB, " +
                    CommandTable.IS_STATUS_OK_COL + " BOOLEAN, " +
                    CommandTable.COMMAND_STATUS_COL + " VARCHAR(512), " +
                    CommandTable.ERROR_COL + " BLOB, " +
                    CommandTable.FAILURE_COL + " BLOB, " +
                    " PRIMARY KEY(" + CommandTable.ID_COL + ')' +
                    ");";

    public CreateCommandTableIfNo(Builder builder) {
        super(builder);
    }

    public static Builder getBuilder(){
        return new Builder();
    }

    public void execute() throws DatabaseException {
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             PreparedStatement statement = connection.prepareStatement(this.query)) {
            statement.execute();
        } catch (SQLException e) {
            //log().error("Exception during table creation:", e);
            throw new DatabaseException(e);
        }
    }

    public static class Builder extends Abstract.Builder<Builder, CreateCommandTableIfNo>{

        @Override
        public CreateCommandTableIfNo build() {
            return new CreateCommandTableIfNo(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }

}