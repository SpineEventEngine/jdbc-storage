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

package org.spine3.server.storage.rdbms;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.server.storage.EntityStorage;
import org.spine3.server.storage.EntityStorageRecord;
import org.spine3.type.TypeName;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.protobuf.Descriptors.Descriptor;
import static org.spine3.protobuf.Messages.*;
import static org.spine3.protobuf.Messages.toAny;

/**
 * The implementation of the entity storage based on the RDBMS.
 *
 * @see JdbcStorageFactory
 * @author Alexander Litus
 */
class JdbcEntityStorage<I, M extends Message> extends EntityStorage<I> implements AutoCloseable {

    /**
     * Entity record column name.
     */
    private static final String ENTITY = "entity";

    /**
     * Entity ID column name.
     */
    private static final String ID = "id";

    @SuppressWarnings("UtilityClass")
    private static class SqlDrafts {

        static final String INSERT_RECORD =
                "INSERT INTO %s " +
                        " (" + ID + ", " + ENTITY + ')' +
                        " VALUES (?, ?);";

        static final String UPDATE_RECORD =
                "UPDATE %s " +
                " SET " + ENTITY + " = ? " +
                " WHERE " + ID + " = ?;";

        static final String SELECT_ALL_BY_ID = "SELECT * FROM %s WHERE " + ID + " = ?;";

        static final String DELETE_ALL = "DELETE FROM  %s ;";

        static final String CREATE_TABLE_IF_DOES_NOT_EXIST =
                "CREATE TABLE IF NOT EXISTS %s (" +
                    ID + " VARCHAR(999), " +
                    ENTITY + " BLOB, " +
                    "PRIMARY KEY(" + ID + ')' +
                ");";
    }

    private static final Pattern PATTERN_DOT = Pattern.compile("\\.");
    private static final String UNDERSCORE = "_";

    private final DataSourceWrapper dataSource;
    private final TypeName typeName;
    private final String tableName;

    private final String insertSql;
    private final String updateSql;
    private final String selectAllByIdSql;
    private final String deleteAllSql;

    /**
     * Creates a new storage instance.
     *
     * @param dataSource the dataSource wrapper
     * @param descriptor the descriptor of the type of messages to save to the storage
     */
    static <I, M extends Message> JdbcEntityStorage<I, M> newInstance(DataSourceWrapper dataSource, Descriptor descriptor) {
        return new JdbcEntityStorage<>(dataSource, descriptor);
    }

    private JdbcEntityStorage(DataSourceWrapper dataSource, Descriptor descriptor) {
        this.dataSource = dataSource;
        this.typeName = TypeName.of(descriptor);
        final String className = typeName.value();
        this.tableName = PATTERN_DOT.matcher(className).replaceAll(UNDERSCORE).toLowerCase();

        this.insertSql = setTableName(SqlDrafts.INSERT_RECORD);
        this.updateSql = setTableName(SqlDrafts.UPDATE_RECORD);
        this.selectAllByIdSql = setTableName(SqlDrafts.SELECT_ALL_BY_ID);
        this.deleteAllSql = setTableName((SqlDrafts.DELETE_ALL));
        final String createTableSql = setTableName(SqlDrafts.CREATE_TABLE_IF_DOES_NOT_EXIST);
        createTableIfDoesNotExist(createTableSql, this.tableName);
    }

    private String setTableName(String sql) {
        return String.format(sql, tableName);
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Nullable
    @Override
    public EntityStorageRecord read(I id) throws DatabaseException {
        final EntityStorageRecord.Id recordId = toRecordId(id);
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             PreparedStatement statement = selectByIdStatement(connection, recordId)) {
            final EntityStorageRecord result = findById(statement);
            return result;
        } catch (SQLException e) {
            logTransactionError(recordId, e);
            throw new DatabaseException(e);
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

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    public void write(EntityStorageRecord record) {
        checkArgument(record.hasState(), "entity state");

        final EntityStorageRecord.Id id = toRecordId(record.getId());
        final byte[] serializedRecord = serialize(record);
        try (ConnectionWrapper connection = dataSource.getConnection(false)) {
            if (containsRecord(id, connection)) {
                update(id, serializedRecord, connection);
            } else {
                insert(id, serializedRecord, connection);
            }
        }
    }

    private static byte[] serialize(Message message) {
        final Any any = toAny(message);
        final byte[] bytes = any.getValue().toByteArray();
        return bytes;
    }

    private void createTableIfDoesNotExist(String sql, String tableName) throws DatabaseException {
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.execute();
        } catch (SQLException e) {
            log().error("Error during table creation, table name = " + tableName, e);
            throw new DatabaseException(e);
        }
    }

    private boolean containsRecord(EntityStorageRecord.Id id, ConnectionWrapper connection) {
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

    private void update(EntityStorageRecord.Id id, byte[] serializedEntity, ConnectionWrapper connection) {
        try (PreparedStatement statement = updateRecordStatement(connection, id, serializedEntity)) {
            statement.execute();
            connection.commit();
        } catch (SQLException e) {
            throw handleDbException(e, id, connection);
        }
    }

    private void insert(EntityStorageRecord.Id id, byte[] serializedEntity, ConnectionWrapper connection) {
        try (PreparedStatement statement = insertRecordStatement(connection, id, serializedEntity)) {
            statement.execute();
            connection.commit();
        } catch (SQLException e) {
            throw handleDbException(e, id, connection);
        }
    }

    @SuppressWarnings("TypeMayBeWeakened")
    private PreparedStatement insertRecordStatement(ConnectionWrapper connection,
                                                    EntityStorageRecord.Id id,
                                                    byte[] serializedRecord) throws SQLException {
        final PreparedStatement statement = connection.prepareStatement(insertSql);
        setEntityId(1, id, statement);
        statement.setBytes(2, serializedRecord);
        return statement;
    }

    private PreparedStatement updateRecordStatement(ConnectionWrapper connection,
                                                    EntityStorageRecord.Id id,
                                                    byte[] serializedEntity) throws SQLException {
        final PreparedStatement statement = connection.prepareStatement(updateSql);
        statement.setBytes(1, serializedEntity);
        setEntityId(2, id, statement);
        return statement;
    }

    private PreparedStatement selectByIdStatement(ConnectionWrapper connection,
                                                  EntityStorageRecord.Id id) throws SQLException {
        final PreparedStatement statement = connection.prepareStatement(selectAllByIdSql);
        setEntityId(1, id, statement);
        return statement;
    }

    @SuppressWarnings("TypeMayBeWeakened")
    private static void setEntityId(int idIndex, EntityStorageRecord.Id id, PreparedStatement statement) throws SQLException {
        final EntityStorageRecord.Id.TypeCase type = id.getTypeCase();
        switch (type) {
            case STRING_VALUE:
                statement.setString(idIndex, id.getStringValue());
                break;
            case LONG_VALUE:
                statement.setLong(idIndex, id.getLongValue());
                break;
            case INT_VALUE:
                statement.setInt(idIndex, id.getIntValue());
                break;
            case TYPE_NOT_SET:
            default:
                throw new IllegalArgumentException("Id type not set.");
        }
    }

    private static DatabaseException handleDbException(SQLException e, EntityStorageRecord.Id id, ConnectionWrapper connection) {
        logTransactionError(id, e);
        connection.rollback();
        throw new DatabaseException(e);
    }

    @Override
    public void close() {
        dataSource.close();
    }

    /**
     * Clears all data in the storage.
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    void clear() throws DatabaseException {
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             final PreparedStatement statement = connection.prepareStatement(deleteAllSql)) {
            statement.execute();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    private static void logTransactionError(EntityStorageRecord.Id id, Exception e) {
        log().error("Error during transaction, entity ID = " + id, e);
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
