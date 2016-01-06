/*
 * Copyright 2015, TeamDev Ltd. All rights reserved.
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

package org.spine3.server.storage.hsqldb;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.protobuf.Messages;
import org.spine3.server.storage.EntityStorage;
import org.spine3.type.TypeName;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;
import static com.google.protobuf.Descriptors.Descriptor;
import static org.spine3.protobuf.Messages.toAny;
import static org.spine3.util.Identifiers.idToString;

/**
 * The implementation of the entity storage based on HyperSQL Database.
 *
 * @see HsqlStorageFactory
 * @author Alexander Litus
 */
@SuppressWarnings("DuplicateStringLiteralInspection")
class HsqlEntityStorage<I, M extends Message> extends EntityStorage<I, M> implements AutoCloseable {

    // TODO:2016-01-04:alexander.litus: have a table per entity type

    static final String ENTITIES = "entities";
    static final String ENTITY = "entity";
    static final String ID = "id";

    private final HsqlDb database;
    private final TypeName typeName;

    /**
     * Creates a new storage instance.
     *
     * @param database   the database wrapper
     * @param descriptor the descriptor of the type of messages to save to the storage
     */
    static <I, M extends Message> HsqlEntityStorage<I, M> newInstance(HsqlDb database, Descriptor descriptor) {
        return new HsqlEntityStorage<>(database, descriptor);
    }

    private HsqlEntityStorage(HsqlDb database, Descriptor descriptor) {
        this.database = database;
        this.typeName = TypeName.of(descriptor);
    }

    /**
     * {@inheritDoc}
     *
     * @throws RuntimeException if a database access error occurs
     */
    @Nullable
    @Override
    public M read(I id) {
        final String idString = idToString(id);
        try (ConnectionWrapper connection = database.getConnection(true);
             PreparedStatement statement = newSelectByIdStatement(connection, idString)) {
            final M result = findById(statement);
            return result;
        } catch (SQLException e) {
            logTransactionError(idString, e);
            throw propagate(e);
        }
    }

    private M findById(PreparedStatement statement) throws SQLException {
        try (ResultSet resultSet = statement.executeQuery()) {
            if (!resultSet.next()) {
                return null;
            }
            final byte[] bytes = resultSet.getBytes(ENTITY);
            final M message = toMessage(bytes);
            return message;
        }
    }

    private M toMessage(byte[] bytes) {
        final Any.Builder builder = Any.newBuilder();
        builder.setTypeUrl(typeName.toTypeUrl());
        builder.setValue(ByteString.copyFrom(bytes));
        final M message = Messages.fromAny(builder.build());
        return message;
    }

    /**
     * {@inheritDoc}
     *
     * @throws RuntimeException if a database access error occurs
     */
    @Override
    public void write(I id, M message) {
        checkNotNull(id, "id");
        checkNotNull(message, "message");

        final String idString = idToString(id);
        final byte[] serializedEntity = serialize(message);
        if (contains(idString)) {
            update(idString, serializedEntity);
        } else {
            insert(idString, serializedEntity);
        }
    }

    private byte[] serialize(M message) {
        final Any any = toAny(message);
        final byte[] bytes = any.getValue().toByteArray();
        return bytes;
    }

    boolean contains(String id) {
        try (ConnectionWrapper connection = database.getConnection(true);
             PreparedStatement statement = newSelectByIdStatement(connection, id)) {
            try (ResultSet resultSet = statement.executeQuery()) {
                final boolean hasNext = resultSet.next();
                return hasNext;
            }
        } catch (SQLException e) {
            logTransactionError(id, e);
            return false;
        }
    }

    private void update(String idString, byte[] serializedEntity) {
        try (ConnectionWrapper connection = database.getConnection(false)) {
            try (PreparedStatement statement = newUpdateEntityStatement(connection, idString, serializedEntity)) {
                statement.execute();
                connection.commit();
            } catch (SQLException e) {
                throw handleDbException(e, idString, connection);
            }
        }
    }

    private void insert(String id, byte[] serializedEntity) {
        try (ConnectionWrapper connection = database.getConnection(false)) {
            try (PreparedStatement statement = newInsertEntityStatement(connection, id, serializedEntity)) {
                statement.execute();
                connection.commit();
            } catch (SQLException e) {
                throw handleDbException(e, id, connection);
            }
        }
    }

    private static final String INSERT_ENTITY_SQL =
            "INSERT INTO " + ENTITIES +
            " (" + ID + ", " + ENTITY + ')' +
            " VALUES (?, ?);";

    private static PreparedStatement newInsertEntityStatement(ConnectionWrapper connection,
                                                              String idString,
                                                              byte[] serializedEntity) throws SQLException {
        final PreparedStatement statement = connection.prepareStatement(INSERT_ENTITY_SQL);
        statement.setString(1, idString);
        statement.setBytes(2, serializedEntity);
        return statement;
    }

    private static final String UPDATE_ENTITY_SQL =
            "UPDATE " + ENTITIES +
            " SET " + ENTITY + " = ? " +
            " WHERE " + ID + " = ?;";

    private static PreparedStatement newUpdateEntityStatement(ConnectionWrapper connection,
                                                              String id,
                                                              byte[] serializedEntity) throws SQLException {
        final PreparedStatement statement = connection.prepareStatement(UPDATE_ENTITY_SQL);
        statement.setBytes(1, serializedEntity);
        statement.setString(2, id);
        return statement;
    }

    private static final String SELECT_ALL_BY_ID_SQL = "SELECT * FROM " + ENTITIES + " WHERE " + ID + " = ?;";

    private static PreparedStatement newSelectByIdStatement(ConnectionWrapper connection, String id) throws SQLException {
        final PreparedStatement statement = connection.prepareStatement(SELECT_ALL_BY_ID_SQL);
        statement.setString(1, id);
        return statement;
    }

    private static RuntimeException handleDbException(SQLException e, String idString, ConnectionWrapper connection) {
        connection.rollback();
        logTransactionError(idString, e);
        throw propagate(e);
    }

    @Override
    public void close() {
        database.close();
    }

    private static void logTransactionError(String idString, SQLException e) {
        log().error("Error during transaction, entity ID = " + idString, e);
    }

    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(HsqlEntityStorage.class);
    }
}
