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
class HsqlEntityStorage<I, M extends Message> extends EntityStorage<I, M> {

    // TODO:2016-01-04:alexander.litus: have a table per entity type

    static final String ENTITIES = "entities";
    static final String ENTITY = "entity";
    static final String ID = "id";

    private static final String SELECT_ALL_BY_ID = "SELECT * FROM " + ENTITIES + " WHERE " + ID + " = ?;";

    private static final String INSERT_ENTITY = "INSERT INTO " + ENTITIES +
            " (" + ID + ", " + ENTITY + ") VALUES (?, ?);";

    private static final String UPDATE_ENTITY = "UPDATE " + ENTITIES +
            " SET " + ID + " = ?, " + ENTITY + " = ? " +
            " WHERE " + ID + " = ?;";

    private static final int ID_PARAM_INDEX = 1;
    private static final int ENTITY_PARAM_INDEX = 2;
    private static final int UPDATE_BY_ID_PARAM_INDEX = 3;

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

    @Nullable
    @Override
    public M read(I id) {
        final String idString = idToString(id);

        try (ConnectionWrapper connection = database.getConnection(true);
             PreparedStatement statement = connection.prepareStatement(SELECT_ALL_BY_ID)) {

            statement.setString(ID_PARAM_INDEX, idString);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    final byte[] bytes = resultSet.getBytes(ENTITY);
                    final Any.Builder builder = Any.newBuilder();
                    builder.setTypeUrl(typeName.toTypeUrl());
                    builder.setValue(ByteString.copyFrom(bytes));

                    final M message = Messages.fromAny(builder.build());
                    return message;
                }
            }
        } catch (SQLException e) {
            logTransactionError(idString, e);
        }
        return null;
    }

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

    boolean contains(String id) {
        try (ConnectionWrapper connection = database.getConnection(true);
             PreparedStatement statement = connection.prepareStatement(SELECT_ALL_BY_ID)) {
            statement.setString(ID_PARAM_INDEX, id);

            try (ResultSet resultSet = statement.executeQuery()) {
                final boolean hasNext = resultSet.next();
                return hasNext;
            }
        } catch (SQLException e) {
            logTransactionError(id, e);
            return false;
        }
    }

    private byte[] serialize(M message) {
        final Any any = toAny(message);
        final byte[] bytes = any.getValue().toByteArray();
        return bytes;
    }

    private void insert(String idString, byte[] serializedEntity) {
        try (final ConnectionWrapper connection = database.getConnection(false)) {

            try (final PreparedStatement statement = connection.prepareStatement(INSERT_ENTITY)) {
                statement.setString(ID_PARAM_INDEX, idString);
                statement.setBytes(ENTITY_PARAM_INDEX, serializedEntity);
                statement.execute();
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                logTransactionError(idString, e);
                propagate(e);
            }
        }
    }

    private void update(String idString, byte[] serializedEntity) {
        try (final ConnectionWrapper connection = database.getConnection(false)) {

            try (final PreparedStatement statement = connection.prepareStatement(UPDATE_ENTITY)) {
                statement.setString(ID_PARAM_INDEX, idString);
                statement.setBytes(ENTITY_PARAM_INDEX, serializedEntity);
                statement.setString(UPDATE_BY_ID_PARAM_INDEX, idString);
                statement.execute();
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                logTransactionError(idString, e);
                propagate(e);
            }
        }
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
