/*
 * Copyright 2016, TeamDev Ltd. All rights reserved.
 *
 * Redistribution and use in source and/or binary forms, with or without
 * modification, must retain the above copyright notice and the following
 * disclaimer.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.spine3.server.storage.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.server.Entity;
import org.spine3.server.EntityId;
import org.spine3.server.storage.EntityStorage;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.DbTableNameFactory;
import org.spine3.server.storage.jdbc.util.IdColumn;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.protobuf.Descriptors.Descriptor;
import static java.lang.String.format;
import static org.spine3.base.Identifiers.idToString;
import static org.spine3.server.storage.jdbc.util.Serializer.readDeserializedRecord;
import static org.spine3.server.storage.jdbc.util.Serializer.serialize;

/**
 * The implementation of the entity storage based on the RDBMS.
 *
 * @param <Id> the type of entity IDs. See {@link EntityId} for details.
 * @see JdbcStorageFactory
 * @author Alexander Litus
 */
/*package*/ class JdbcEntityStorage<Id> extends EntityStorage<Id> {

    @SuppressWarnings("DuplicateStringLiteralInspection")
    private interface SQL {

        /**
         * Entity record column name.
         */
        String ENTITY = "entity";

        /**
         * Entity ID column name.
         */
        String ID = "id";

        String INSERT =
                "INSERT INTO %s " +
                " (" + ID + ", " + ENTITY + ')' +
                " VALUES (?, ?);";

        String UPDATE =
                "UPDATE %s " +
                " SET " + ENTITY + " = ? " +
                " WHERE " + ID + " = ?;";

        String SELECT_BY_ID = "SELECT " + ENTITY + " FROM %s WHERE " + ID + " = ?;";

        String DELETE_ALL = "DELETE FROM %s;";

        String CREATE_TABLE_IF_DOES_NOT_EXIST =
                "CREATE TABLE IF NOT EXISTS %s (" +
                    ID + " %s, " +
                    ENTITY + " BLOB, " +
                    "PRIMARY KEY(" + ID + ')' +
                ");";
    }

    private static final Descriptor RECORD_DESCRIPTOR = EntityStorageRecord.getDescriptor();

    private final DataSourceWrapper dataSource;

    private final IdColumn<Id> idColumn;

    private final InsertQuery insertQuery;
    private final UpdateQuery updateQuery;
    private final SelectByIdQuery selectByIdQuery;
    private final DeleteAllQuery deleteAllQuery;

    /**
     * Creates a new storage instance.
     *
     * @param dataSource the dataSource wrapper
     * @param entityClass the class of entities to save to the storage
     */
    /*package*/ static <Id> JdbcEntityStorage<Id> newInstance(DataSourceWrapper dataSource,
                                                               Class<? extends Entity<Id, ?>> entityClass) {
        return new JdbcEntityStorage<>(dataSource, entityClass);
    }

    private JdbcEntityStorage(DataSourceWrapper dataSource, Class<? extends Entity<Id, ?>> entityClass) {
        this.dataSource = dataSource;
        final String tableName = DbTableNameFactory.newTableName(entityClass);
        this.insertQuery = new InsertQuery(tableName);
        this.updateQuery = new UpdateQuery(tableName);
        this.selectByIdQuery = new SelectByIdQuery(tableName);
        this.deleteAllQuery = new DeleteAllQuery(tableName);
        this.idColumn = IdColumn.newInstance(entityClass);
        new CreateTableIfDoesNotExistQuery().execute(tableName);
    }

    private abstract class WriteQuery {

        protected void execute(ConnectionWrapper connection, Id id, byte[] serializedRecord) {
            try (PreparedStatement statement = statement(connection, id, serializedRecord)) {
                statement.execute();
                connection.commit();
            } catch (SQLException e) {
                logTransactionError(id, e);
                connection.rollback();
                throw new DatabaseException(e);
            }
        }

        protected abstract PreparedStatement statement(ConnectionWrapper connection, Id id, byte[] serializedRecord);
    }

    private class InsertQuery extends WriteQuery {

        private final String insertQuery;

        private InsertQuery(String tableName) {
            this.insertQuery = format(SQL.INSERT, tableName);
        }

        @Override
        protected PreparedStatement statement(ConnectionWrapper connection, Id id, byte[] serializedRecord) {
            try {
                final PreparedStatement statement = connection.prepareStatement(insertQuery);
                idColumn.setId(1, id, statement);
                statement.setBytes(2, serializedRecord);
                return statement;
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }
    }

    private class UpdateQuery extends WriteQuery {

        private final String updateQuery;

        private UpdateQuery(String tableName) {
            this.updateQuery = format(SQL.UPDATE, tableName);
        }

        @Override
        protected PreparedStatement statement(ConnectionWrapper connection, Id id, byte[] serializedRecord) {
            try {
                final PreparedStatement statement = connection.prepareStatement(updateQuery);
                statement.setBytes(1, serializedRecord);
                idColumn.setId(2, id, statement);
                return statement;
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }
    }

    private class SelectByIdQuery {

        private final String selectByIdQuery;

        private SelectByIdQuery(String tableName) {
            this.selectByIdQuery = format(SQL.SELECT_BY_ID, tableName);
        }

        private PreparedStatement statement(ConnectionWrapper connection, Id id) {
            final PreparedStatement statement = connection.prepareStatement(selectByIdQuery);
            idColumn.setId(1, id, statement);
            return statement;
        }
    }

    @SuppressWarnings("InnerClassMayBeStatic") // not static to be consistent
    private class DeleteAllQuery {

        private final String deleteAllQuery;

        private DeleteAllQuery(String tableName) {
            this.deleteAllQuery = format(SQL.DELETE_ALL, tableName);
        }

        public PreparedStatement statement(ConnectionWrapper connection) {
            final PreparedStatement statement = connection.prepareStatement(deleteAllQuery);
            return statement;
        }
    }

    private class CreateTableIfDoesNotExistQuery {

        private void execute(String tableName) throws DatabaseException {
            final String idColumnType = idColumn.getColumnDataType();
            final String createTableSql = format(SQL.CREATE_TABLE_IF_DOES_NOT_EXIST, tableName, idColumnType);
            try (ConnectionWrapper connection = dataSource.getConnection(true);
                 PreparedStatement statement = connection.prepareStatement(createTableSql)) {
                statement.execute();
            } catch (SQLException e) {
                log().error("Error while creating a table with the name: " + tableName, e);
                throw new DatabaseException(e);
            }
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Nullable
    @Override
    protected EntityStorageRecord readInternal(Id id) throws DatabaseException {
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             PreparedStatement statement = selectByIdQuery.statement(connection, id)) {
            final EntityStorageRecord result = readDeserializedRecord(statement, SQL.ENTITY, RECORD_DESCRIPTOR);
            return result;
        } catch (SQLException e) {
            logTransactionError(id, e);
            throw new DatabaseException(e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    protected void writeInternal(Id id, EntityStorageRecord record) throws DatabaseException {
        checkArgument(record.hasState(), "entity state");

        final byte[] serializedRecord = serialize(record);
        try (ConnectionWrapper connection = dataSource.getConnection(false)) {
            if (containsRecord(connection, id)) {
                updateQuery.execute(connection, id, serializedRecord);
            } else {
                insertQuery.execute(connection, id, serializedRecord);
            }
        }
    }

    private boolean containsRecord(ConnectionWrapper connection, Id id) {
        try (PreparedStatement statement = selectByIdQuery.statement(connection, id);
             ResultSet resultSet = statement.executeQuery()) {
            final boolean hasNext = resultSet.next();
            return hasNext;
        } catch (SQLException e) {
            logTransactionError(id, e);
            connection.rollback();
            return false;
        }
    }

    @Override
    public void close() throws DatabaseException {
        dataSource.close();
        try {
            super.close();
        } catch (Exception e) {
            throw new DatabaseException(e);
        }
    }

    /**
     * Clears all data in the storage.
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    /*package*/ void clear() throws DatabaseException {
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             final PreparedStatement statement = deleteAllQuery.statement(connection)) {
            statement.execute();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    private void logTransactionError(Id id, Exception e) {
        log().error("Error during transaction, entity ID = " + idToString(id), e);
    }

    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(JdbcEntityStorage.class);
    }
}
