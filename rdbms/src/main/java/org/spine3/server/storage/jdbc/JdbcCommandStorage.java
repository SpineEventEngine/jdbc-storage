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

import com.google.protobuf.Descriptors.Descriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.base.CommandId;
import org.spine3.server.storage.CommandStorage;
import org.spine3.server.storage.CommandStorageRecord;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.server.storage.jdbc.util.Serializer.readDeserializedRecord;
import static org.spine3.server.storage.jdbc.util.Serializer.serialize;

/**
 * The implementation of the command storage based on the RDBMS.
 *
 * @see JdbcStorageFactory
 * @author Alexander Litus
 */
/*package*/ class JdbcCommandStorage extends CommandStorage {

    /**
     * Commands table name.
     */
    private static final String TABLE_NAME = "commands";

    /**
     * Command ID column name.
     */
    private static final String ID_COL = "id";

    /**
     * Command record column name.
     */
    private static final String COMMAND_COL = "command";

    private static final Descriptor RECORD_DESCRIPTOR = CommandStorageRecord.getDescriptor();

    private final DataSourceWrapper dataSource;

    /**
     * Creates a new storage instance.
     *
     * @param dataSource a data source to use to obtain connections
     * @throws DatabaseException if an error occurs during an interaction with the DB
     * @return a new storage instance
     */
    /*package*/ static CommandStorage newInstance(DataSourceWrapper dataSource) throws DatabaseException {
        return new JdbcCommandStorage(dataSource);
    }

    private JdbcCommandStorage(DataSourceWrapper dataSource) throws DatabaseException {
        this.dataSource = dataSource;
        CreateTableIfDoesNotExistQuery.execute(dataSource);
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if the storage is closed
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    public void write(CommandId commandId, CommandStorageRecord record) throws DatabaseException {
        checkNotNull(commandId);
        checkNotClosed();

        final String id = commandId.getUuid();
        try (ConnectionWrapper connection = dataSource.getConnection(false)) {
            try (PreparedStatement statement = InsertQuery.statement(connection, id, record)) {
                statement.execute();
                connection.commit();
            } catch (SQLException e) {
                log().error("Error while writing command record, command ID = " + commandId, e);
                connection.rollback();
                throw new DatabaseException(e);
            }
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if the storage is closed
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Nullable
    @Override
    @SuppressWarnings("RefusedBequest") // the method from the superclass throws an UnsupportedOperationException
    public CommandStorageRecord read(CommandId commandId) throws DatabaseException {
        checkNotClosed();
        final String id = commandId.getUuid();
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             PreparedStatement statement = SelectCommandByIdQuery.statement(connection, id)) {
            final CommandStorageRecord record = readDeserializedRecord(statement, COMMAND_COL, RECORD_DESCRIPTOR);
            return record;
        } catch (SQLException e) {
            log().error("Error while reading command record, command ID = " + commandId, e);
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

    @SuppressWarnings({"UtilityClass", "DuplicateStringLiteralInspection"})
    private static class CreateTableIfDoesNotExistQuery {

        private static final String CREATE_TABLE_QUERY =
                "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (" +
                    ID_COL + " VARCHAR(512), " +
                    COMMAND_COL + " BLOB, " +
                    " PRIMARY KEY(" + ID_COL + ')' +
                ");";

        private static void execute(DataSourceWrapper dataSource) throws DatabaseException {
            try (ConnectionWrapper connection = dataSource.getConnection(true);
                 PreparedStatement statement = connection.prepareStatement(CREATE_TABLE_QUERY)) {
                statement.execute();
            } catch (SQLException e) {
                log().error("Exception during table creation:", e);
                throw new DatabaseException(e);
            }
        }
    }

    @SuppressWarnings({"UtilityClass", "DuplicateStringLiteralInspection"})
    private static class InsertQuery {

        private static final String INSERT_QUERY =
                "INSERT INTO " + TABLE_NAME + " (" +
                ID_COL + ", " +
                COMMAND_COL +
                ") VALUES (?, ?);";

        private static PreparedStatement statement(ConnectionWrapper connection,
                                                   String commandId,
                                                   CommandStorageRecord record) {
            final PreparedStatement statement = connection.prepareStatement(INSERT_QUERY);
            try {
                statement.setString(1, commandId);
                final byte[] serializedRecord = serialize(record);
                statement.setBytes(2, serializedRecord);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
            return statement;
        }
    }

    @SuppressWarnings({"UtilityClass", "DuplicateStringLiteralInspection"})
    private static class SelectCommandByIdQuery {

        private static final String SELECT_QUERY =
                "SELECT " + COMMAND_COL +
                " FROM " + TABLE_NAME +
                " WHERE " + ID_COL + " = ?;";

        private static PreparedStatement statement(ConnectionWrapper connection, String id) {
            try {
                final PreparedStatement statement = connection.prepareStatement(SELECT_QUERY);
                statement.setString(1, id);
                return statement;
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }
    }

    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(JdbcCommandStorage.class);
    }
}
