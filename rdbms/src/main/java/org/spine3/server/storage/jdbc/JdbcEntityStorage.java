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
import static org.spine3.server.storage.jdbc.util.Serializer.readAndDeserializeMessage;
import static org.spine3.server.storage.jdbc.util.Serializer.serialize;

/**
 * The implementation of the entity storage based on the RDBMS.
 *
 * @param <Id> the type of entity IDs. See {@link EntityId} for details.
 * @see JdbcStorageFactory
 * @author Alexander Litus
 */
/*package*/ class JdbcEntityStorage<Id> extends EntityStorage<Id> {

    /**
     * Entity record column name.
     */
    private static final String ENTITY_COL = "entity";

    /**
     * Entity ID column name.
     */
    private static final String ID_COL = "id";

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
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    /*package*/ static <Id> JdbcEntityStorage<Id> newInstance(DataSourceWrapper dataSource,
                                                              Class<? extends Entity<Id, ?>> entityClass)
                                                              throws DatabaseException {
        return new JdbcEntityStorage<>(dataSource, entityClass);
    }

    private JdbcEntityStorage(DataSourceWrapper dataSource, Class<? extends Entity<Id, ?>> entityClass)
            throws DatabaseException {
        this.dataSource = dataSource;
        final String tableName = DbTableNameFactory.newTableName(entityClass);
        this.insertQuery = new InsertQuery(tableName);
        this.updateQuery = new UpdateQuery(tableName);
        this.selectByIdQuery = new SelectByIdQuery(tableName);
        this.deleteAllQuery = new DeleteAllQuery(tableName);
        this.idColumn = IdColumn.newInstance(entityClass);
        new CreateTableIfDoesNotExistQuery().execute(tableName);
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
            final EntityStorageRecord result = readAndDeserializeMessage(statement, ENTITY_COL, RECORD_DESCRIPTOR);
            return result;
        } catch (SQLException e) {
            logTransactionError("reading record", id, e);
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
        if (containsRecord(id)) {
            updateQuery.execute(id, serializedRecord);
        } else {
            insertQuery.execute(id, serializedRecord);
        }
    }

    private boolean containsRecord(Id id) throws DatabaseException {
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             PreparedStatement statement = selectByIdQuery.statement(connection, id);
             ResultSet resultSet = statement.executeQuery()) {
            final boolean hasNext = resultSet.next();
            return hasNext;
        } catch (SQLException e) {
            logTransactionError("checking record", id, e);
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

    private abstract class WriteQuery {

        private final String query;
        private final int idIndexInQuery;
        private final int entityIndexInQuery;

        protected WriteQuery(String query, int idIndexInQuery, int entityIndexInQuery) {
            this.query = query;
            this.idIndexInQuery = idIndexInQuery;
            this.entityIndexInQuery = entityIndexInQuery;
        }

        protected void execute(Id id, byte[] serializedRecord) {
            try (ConnectionWrapper connection = dataSource.getConnection(false)) {
                try (PreparedStatement statement = statement(connection, id, serializedRecord)) {
                    statement.execute();
                    connection.commit();
                } catch (SQLException e) {
                    logTransactionError("writing record", id, e);
                    connection.rollback();
                    throw new DatabaseException(e);
                }
            }
        }

        private PreparedStatement statement(ConnectionWrapper connection, Id id, byte[] serializedRecord) {
            try {
                final PreparedStatement statement = connection.prepareStatement(query);
                idColumn.setId(idIndexInQuery, id, statement);
                statement.setBytes(entityIndexInQuery, serializedRecord);
                return statement;
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }
    }

    private class InsertQuery extends WriteQuery {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String INSERT =
                "INSERT INTO %s " +
                " (" + ID_COL + ", " + ENTITY_COL + ')' +
                " VALUES (?, ?);";

        private static final int ID_INDEX_IN_QUERY = 1;
        private static final int ENTITY_INDEX_IN_QUERY = 2;

        private InsertQuery(String tableName) {
            super(format(INSERT, tableName), ID_INDEX_IN_QUERY, ENTITY_INDEX_IN_QUERY);
        }
    }

    private class UpdateQuery extends WriteQuery {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String UPDATE =
                "UPDATE %s " +
                " SET " + ENTITY_COL + " = ? " +
                " WHERE " + ID_COL + " = ?;";

        private static final int ID_INDEX_IN_QUERY = 2;
        private static final int ENTITY_INDEX_IN_QUERY = 1;

        private UpdateQuery(String tableName) {
            super(format(UPDATE, tableName), ID_INDEX_IN_QUERY, ENTITY_INDEX_IN_QUERY);
        }
    }

    private class SelectByIdQuery {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String SELECT_BY_ID = "SELECT " + ENTITY_COL + " FROM %s WHERE " + ID_COL + " = ?;";

        private final String selectByIdQuery;

        private SelectByIdQuery(String tableName) {
            this.selectByIdQuery = format(SELECT_BY_ID, tableName);
        }

        private PreparedStatement statement(ConnectionWrapper connection, Id id) {
            final PreparedStatement statement = connection.prepareStatement(selectByIdQuery);
            idColumn.setId(1, id, statement);
            return statement;
        }
    }

    private static class DeleteAllQuery {

        private static final String DELETE_ALL = "DELETE FROM %s ;";

        private final String deleteAllQuery;

        private DeleteAllQuery(String tableName) {
            this.deleteAllQuery = format(DELETE_ALL, tableName);
        }

        public PreparedStatement statement(ConnectionWrapper connection) {
            final PreparedStatement statement = connection.prepareStatement(deleteAllQuery);
            return statement;
        }
    }

    private class CreateTableIfDoesNotExistQuery {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String CREATE_TABLE_IF_DOES_NOT_EXIST =
                "CREATE TABLE IF NOT EXISTS %s (" +
                    ID_COL + " %s, " +
                    ENTITY_COL + " BLOB, " +
                    "PRIMARY KEY(" + ID_COL + ')' +
                ");";

        private void execute(String tableName) throws DatabaseException {
            final String idColumnType = idColumn.getColumnDataType();
            final String createTableSql = format(CREATE_TABLE_IF_DOES_NOT_EXIST, tableName, idColumnType);
            try (ConnectionWrapper connection = dataSource.getConnection(true);
                 PreparedStatement statement = connection.prepareStatement(createTableSql)) {
                statement.execute();
            } catch (SQLException e) {
                log().error("Error while creating a table with the name: " + tableName, e);
                throw new DatabaseException(e);
            }
        }
    }

    private void logTransactionError(String actionName, Id id, Exception e) {
        log().error("Error during " + actionName + ", entity ID = " + idToString(id), e);
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
