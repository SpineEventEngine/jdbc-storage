package org.spine3.server.storage.jdbc.query.tables.event;

import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.AbstractQuery;
import org.spine3.server.storage.jdbc.query.constants.EventTable;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static java.lang.String.format;

public class CreateTableIfDoesNotExistQuery extends AbstractQuery {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private static final String CREATE_TABLE_QUERY =
            "CREATE TABLE IF NOT EXISTS " + EventTable.TABLE_NAME + " (" +
                    EventTable.EVENT_ID_COL + " VARCHAR(512), " +
                    EventTable.EVENT_COL + " BLOB, " +
                    EventTable.EVENT_TYPE_COL + " VARCHAR(512), " +
                    EventTable.PRODUCER_ID_COL + " VARCHAR(512), " +
                    EventTable.SECONDS_COL + " BIGINT, " +
                    EventTable.NANOSECONDS_COL + " INT, " +
                    " PRIMARY KEY(" + EventTable.EVENT_ID_COL + ')' +
                    ");";

    protected CreateTableIfDoesNotExistQuery(Builder builder) {
        super(builder);
    }

    public void execute() throws DatabaseException {
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             PreparedStatement statement = prepareStatement(connection)) {
            statement.execute();
        } catch (SQLException e) {
            //log().error("Error while creating a table with the name: " + tableName, e);
            throw new DatabaseException(e);
        }
    }

    public static Builder getBuilder() {
        final Builder builder = new Builder();
        builder.setQuery(CREATE_TABLE_QUERY);
        return builder;
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