package org.spine3.server.storage.jdbc.query;

import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class UpdateRecord <Id> extends AbstractQuery implements Write{

    private final Id id;
    private int idIndexInQuery;
    private final IdColumn<Id> idColumn;

    protected UpdateRecord(Builder<? extends Builder, ? extends UpdateRecord, Id> builder) {
        super(builder);
        this.idIndexInQuery = builder.idIndexInQuery;
        this.idColumn = builder.idColumn;
        this.id = builder.id;
    }

    @Override
    public void execute() throws DatabaseException {
        try (ConnectionWrapper connection = this.dataSource.getConnection(false)) {
            try (PreparedStatement statement = prepareStatement(connection)) {
                statement.execute();
                connection.commit();
            } catch (SQLException e) {
                // logError(e);
                connection.rollback();
                throw new DatabaseException(e);
            }
        }
    }

    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);

        idColumn.setId(idIndexInQuery, id, statement);

        return statement;
    }

    public abstract static class Builder<B extends Builder<B, Q, Id>, Q extends UpdateRecord, Id>
            extends AbstractQuery.Builder<B, Q>{
        private int idIndexInQuery;
        private IdColumn<Id> idColumn;
        private Id id;

        public B setId(Id id) {
            this.id = id;
            return getThis();
        }

        public B setIdColumn(IdColumn<Id> idColumn) {
            this.idColumn = idColumn;
            return getThis();
        }

        public B setIdIndexInQuery(int idIndexInQuery) {
            this.idIndexInQuery = idIndexInQuery;
            return getThis();
        }
    }

}
