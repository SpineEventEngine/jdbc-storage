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
import org.spine3.base.Event;
import org.spine3.base.EventId;
import org.spine3.server.event.EventStreamQuery;
import org.spine3.server.storage.EventStorage;
import org.spine3.server.storage.EventStorageRecord;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.type.TypeName;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

import static org.spine3.protobuf.Messages.fromAny;
import static org.spine3.protobuf.Messages.toAny;

/**
 * The implementation of the event storage based on the RDBMS.
 *
 * @see JdbcStorageFactory
 * @author Alexander Litus
 */
public class JdbcEventStorage extends EventStorage {

    @SuppressWarnings({"UtilityClass", "DuplicateStringLiteralInspection", "ClassNamingConvention"})
    private static class SQL {

        /**
         * Events table name.
         */
        static final String TABLE_NAME = "events";

        /**
         * Event ID column name.
         */
        static final String ID = "id";

        /**
         * Event record column name.
         */
        static final String EVENT = "event";

        static final String INSERT_RECORD =
                "INSERT INTO " + TABLE_NAME +
                " (" + ID + ", " + EVENT + ')' +
                " VALUES (?, ?);";

        static final String UPDATE_RECORD =
                "UPDATE " + TABLE_NAME +
                " SET " + EVENT + " = ? " +
                " WHERE " + ID + " = ?;";

        static final String SELECT_BY_ID =
                "SELECT " + EVENT +
                " FROM " + TABLE_NAME +
                " WHERE " + ID + " = ?;";

        static final String CREATE_TABLE_IF_DOES_NOT_EXIST =
                "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (" +
                    ID + " VARCHAR(256), " +
                    EVENT + " BLOB, " +
                    "PRIMARY KEY(" + ID + ')' +
                ");";
    }

    private final DataSourceWrapper dataSource;

    /**
     * Creates a new storage instance.
     *
     * @param dataSource the dataSource wrapper
     */
    /*package*/ static JdbcEventStorage newInstance(DataSourceWrapper dataSource) {
        return new JdbcEventStorage(dataSource);
    }

    private JdbcEventStorage(DataSourceWrapper dataSource) {
        this.dataSource = dataSource;
        createTableIfDoesNotExist();
    }

    private void createTableIfDoesNotExist() throws DatabaseException {
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             PreparedStatement statement = connection.prepareStatement(SQL.CREATE_TABLE_IF_DOES_NOT_EXIST)) {
            statement.execute();
        } catch (SQLException e) {
            log().error("Error during table creation:", e);
            throw new DatabaseException(e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    public Iterator<Event> iterator(EventStreamQuery query) throws DatabaseException {
        return null;
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    protected void writeInternal(EventStorageRecord record) throws DatabaseException {
        final String id = record.getEventId();
        final byte[] serializedRecord = serialize(record);
        try (ConnectionWrapper connection = dataSource.getConnection(false)) {
            try (PreparedStatement statement = insertRecordStatement(connection, id, serializedRecord)) {
                statement.execute();
                connection.commit();
            } catch (SQLException e) {
                log().error("Error during transaction, event ID = " + id, e);
                connection.rollback();
                throw new DatabaseException(e);
            }
        }
    }

    // TODO:2016-02-03:alexander.litus: extract
    private static byte[] serialize(Message message) {
        final Any any = toAny(message);
        final byte[] bytes = any.getValue().toByteArray();
        return bytes;
    }

    /**
     * {@inheritDoc}
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Nullable
    @Override
    protected EventStorageRecord readInternal(EventId eventId) throws DatabaseException {
        final String id = eventId.getUuid();
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             PreparedStatement statement = selectByIdStatement(connection, id)) {
            final EventStorageRecord result = findById(statement);
            return result;
        } catch (SQLException e) {
            log().error("Error during transaction, event ID = " + id, e);
            throw new DatabaseException(e);
        }
    }

    // TODO:2016-02-03:alexander.litus: extract
    private static EventStorageRecord findById(PreparedStatement statement) throws DatabaseException {
        try (ResultSet resultSet = statement.executeQuery()) {
            if (!resultSet.next()) {
                return null;
            }
            final byte[] bytes = resultSet.getBytes(SQL.EVENT);
            final EventStorageRecord record = toRecord(bytes);
            return record;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    // TODO:2016-02-03:alexander.litus: extract
    private static EventStorageRecord toRecord(byte[] bytes) {
        final Any.Builder builder = Any.newBuilder();
        builder.setTypeUrl(TypeName.of(EventStorageRecord.getDescriptor()).toTypeUrl());
        builder.setValue(ByteString.copyFrom(bytes));
        final EventStorageRecord message = fromAny(builder.build());
        return message;
    }

    private static PreparedStatement insertRecordStatement(ConnectionWrapper connection,
                                                           String id,
                                                           byte[] serializedRecord) {
        final PreparedStatement statement = connection.prepareStatement(SQL.INSERT_RECORD);
        try {
            statement.setString(1, id);
            statement.setBytes(2, serializedRecord);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
        return statement;
    }

    private static PreparedStatement selectByIdStatement(ConnectionWrapper connection, String id){
        try {
            final PreparedStatement statement = connection.prepareStatement(SQL.SELECT_BY_ID);
            statement.setString(1, id);
            return statement;
        } catch (SQLException e) {
            throw new DatabaseException(e);
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

    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(JdbcEventStorage.class);
    }
}
