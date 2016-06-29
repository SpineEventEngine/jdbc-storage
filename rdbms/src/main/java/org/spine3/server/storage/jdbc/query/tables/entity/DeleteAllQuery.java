package org.spine3.server.storage.jdbc.query.tables.entity;

import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.query.AbstractQuery;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static java.lang.String.format;

public class DeleteAllQuery extends AbstractQuery {

    private static final String DELETE_ALL = "DELETE FROM %s ;";

    private DeleteAllQuery(Builder builder) {
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

    public static Builder getBuilder(String tableName) {
        final Builder builder = new Builder();
        builder.setQuery(format(DELETE_ALL, tableName));
        return builder;
    }

    public static class Builder extends AbstractQuery.Builder<Builder, DeleteAllQuery> {

        @Override
        public DeleteAllQuery build() {
            return new DeleteAllQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}