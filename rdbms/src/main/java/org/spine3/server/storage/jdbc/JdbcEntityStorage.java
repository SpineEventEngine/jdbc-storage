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

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.server.Entity;
import org.spine3.server.EntityId;
import org.spine3.server.storage.EntityStorage;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.DbTableNamesEscaper;
import org.spine3.server.storage.jdbc.util.IdHelper;
import org.spine3.type.TypeName;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.spine3.protobuf.Messages.fromAny;
import static org.spine3.protobuf.Messages.toAny;
import static org.spine3.server.Identifiers.idToString;

/**
 * The implementation of the entity storage based on the RDBMS.
 *
 * @param <I> the type of entity IDs. See {@link EntityId} for details.
 * @see JdbcStorageFactory
 * @author Alexander Litus
 */
class JdbcEntityStorage<I> extends EntityStorage<I> {

    /**
     * Entity record column name.
     */
    private static final String ENTITY = "entity";

    /**
     * Entity ID column name.
     */
    private static final String ID = "id";

    @SuppressWarnings({"UtilityClass", "DuplicateStringLiteralInspection"})
    private static class SqlDrafts {

        static final String INSERT_RECORD =
                "INSERT INTO %s " +
                " (" + ID + ", " + ENTITY + ')' +
                " VALUES (?, ?);";

        static final String UPDATE_RECORD =
                "UPDATE %s " +
                " SET " + ENTITY + " = ? " +
                " WHERE " + ID + " = ?;";

        static final String SELECT_BY_ID = "SELECT " + ENTITY + " FROM %s WHERE " + ID + " = ?;";

        static final String DELETE_ALL = "DELETE FROM %s;";

        static final String CREATE_TABLE_IF_DOES_NOT_EXIST =
                "CREATE TABLE IF NOT EXISTS %s (" +
                    ID + " %s, " +
                    ENTITY + " BLOB, " +
                    "PRIMARY KEY(" + ID + ')' +
                ");";
    }

    private final DataSourceWrapper dataSource;

    private final IdHelper<I> idHelper;

    private final String insertSql;
    private final String updateSql;
    private final String selectByIdSql;
    private final String deleteAllSql;

    /**
     * Creates a new storage instance.
     *
     * @param dataSource the dataSource wrapper
     * @param entityClass the class of entities to save to the storage
     */
    /* package */ static <I> JdbcEntityStorage<I> newInstance(DataSourceWrapper dataSource,
                                                              Class<? extends Entity<I, ?>> entityClass) {
        return new JdbcEntityStorage<>(dataSource, entityClass);
    }

    private JdbcEntityStorage(DataSourceWrapper dataSource, Class<? extends Entity<I, ?>> entityClass) {
        this.dataSource = dataSource;

        final String tableName = DbTableNamesEscaper.toTableName(entityClass);
        this.insertSql = format(SqlDrafts.INSERT_RECORD, tableName);
        this.updateSql = format(SqlDrafts.UPDATE_RECORD, tableName);
        this.selectByIdSql = format(SqlDrafts.SELECT_BY_ID, tableName);
        this.deleteAllSql = format(SqlDrafts.DELETE_ALL, tableName);

        this.idHelper = IdHelper.newInstance(entityClass);
        createTableIfDoesNotExist(tableName);
    }

    private void createTableIfDoesNotExist(String tableName) throws DatabaseException {
        final String idColumnType = idHelper.getIdColumnType();
        final String createTableSql = format(SqlDrafts.CREATE_TABLE_IF_DOES_NOT_EXIST, tableName, idColumnType);
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             PreparedStatement statement = connection.prepareStatement(createTableSql)) {
            statement.execute();
        } catch (SQLException e) {
            log().error("Error while creating a table with the name: " + tableName, e);
            throw new DatabaseException(e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Nullable
    @Override
    protected EntityStorageRecord readInternal(I id) throws DatabaseException {
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             PreparedStatement statement = selectByIdStatement(connection, id)) {
            final EntityStorageRecord result = findById(statement);
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
    protected void writeInternal(I id, EntityStorageRecord record) throws DatabaseException {
        checkArgument(record.hasState(), "entity state");

        final byte[] serializedRecord = serialize(record);
        try (ConnectionWrapper connection = dataSource.getConnection(false)) {
            if (containsRecord(connection, id)) {
                update(connection, id, serializedRecord);
            } else {
                insert(connection, id, serializedRecord);
            }
        }
    }

    private static EntityStorageRecord findById(PreparedStatement statement) throws DatabaseException {
        try (ResultSet resultSet = statement.executeQuery()) {
            if (!resultSet.next()) {
                return null;
            }
            final byte[] bytes = resultSet.getBytes(ENTITY);
            final EntityStorageRecord record = toEntityRecord(bytes);
            return record;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    private static EntityStorageRecord toEntityRecord(byte[] bytes) {
        final Any.Builder builder = Any.newBuilder();
        builder.setTypeUrl(TypeName.of(EntityStorageRecord.getDescriptor()).toTypeUrl());
        builder.setValue(ByteString.copyFrom(bytes));
        final EntityStorageRecord message = fromAny(builder.build());
        return message;
    }

    private static byte[] serialize(Message message) {
        final Any any = toAny(message);
        final byte[] bytes = any.getValue().toByteArray();
        return bytes;
    }

    private boolean containsRecord(ConnectionWrapper connection, I id) {
        try (PreparedStatement statement = selectByIdStatement(connection, id);
             ResultSet resultSet = statement.executeQuery()) {
            final boolean hasNext = resultSet.next();
            return hasNext;
        } catch (SQLException e) {
            logTransactionError(id, e);
            connection.rollback();
            return false;
        }
    }

    private void update(ConnectionWrapper connection, I id, byte[] serializedEntity) {
        try (PreparedStatement statement = updateRecordStatement(connection, id, serializedEntity)) {
            statement.execute();
            connection.commit();
        } catch (SQLException e) {
            throw handleDbException(connection, e, id);
        }
    }

    private void insert(ConnectionWrapper connection, I id, byte[] serializedEntity) {
        try (PreparedStatement statement = insertRecordStatement(connection, id, serializedEntity)) {
            statement.execute();
            connection.commit();
        } catch (SQLException e) {
            throw handleDbException(connection, e, id);
        }
    }

    private PreparedStatement insertRecordStatement(ConnectionWrapper connection, I id, byte[] serializedRecord) {
        try {
            final PreparedStatement statement = connection.prepareStatement(insertSql);
            idHelper.setId(1, id, statement);
            statement.setBytes(2, serializedRecord);
            return statement;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    private PreparedStatement updateRecordStatement(ConnectionWrapper connection, I id, byte[] serializedEntity) {
        try {
            final PreparedStatement statement = connection.prepareStatement(updateSql);
            statement.setBytes(1, serializedEntity);
            idHelper.setId(2, id, statement);
            return statement;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    private PreparedStatement selectByIdStatement(ConnectionWrapper connection, I id) {
        final PreparedStatement statement = connection.prepareStatement(selectByIdSql);
        idHelper.setId(1, id, statement);
        return statement;
    }

    private DatabaseException handleDbException(ConnectionWrapper connection, SQLException e, I id) {
        logTransactionError(id, e);
        connection.rollback();
        throw new DatabaseException(e);
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
             final PreparedStatement statement = connection.prepareStatement(deleteAllSql)) {
            statement.execute();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    private void logTransactionError(I id, Exception e) {
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
