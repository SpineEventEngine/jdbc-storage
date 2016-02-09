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

    @SuppressWarnings({"UtilityClass", "DuplicateStringLiteralInspection", "UtilityClassWithoutPrivateConstructor"})
    private interface SQL {

        /**
         * Commands table name.
         */
        String TABLE_NAME = "commands";

        /**
         * Command ID column name.
         */
        String COMMAND_ID = "id";

        /**
         * Command record column name.
         */
        String COMMAND = "command";

        class CreateTableIfDoesNotExist {

            private static final String CREATE_TABLE_QUERY =
                    "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (" +
                        COMMAND_ID + " VARCHAR(512), " +
                        COMMAND + " BLOB, " +
                        " PRIMARY KEY(" + COMMAND_ID + ')' +
                    ");";

            private static PreparedStatement statement(ConnectionWrapper connection) {
                return connection.prepareStatement(CREATE_TABLE_QUERY);
            }
        }

        class Insert {

            private static final String INSERT_QUERY =
                    "INSERT INTO " + TABLE_NAME + " (" +
                        COMMAND_ID + ", " +
                        COMMAND +
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

        class SelectCommandById {

            private static final String SELECT_QUERY =
                    "SELECT " + COMMAND +
                    " FROM " + TABLE_NAME +
                    " WHERE " + COMMAND_ID + " = ?;";

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
    }

    private static final Descriptor RECORD_DESCRIPTOR = CommandStorageRecord.getDescriptor();

    private final DataSourceWrapper dataSource;

    /*package*/ static CommandStorage newInstance(DataSourceWrapper dataSource) {
        return new JdbcCommandStorage(dataSource);
    }

    private JdbcCommandStorage(DataSourceWrapper dataSource) {
        this.dataSource = dataSource;
        createTableIfDoesNotExist();
    }

    private void createTableIfDoesNotExist() throws DatabaseException {
        try (ConnectionWrapper connection = dataSource.getConnection(true);
             PreparedStatement statement = SQL.CreateTableIfDoesNotExist.statement(connection)) {
            statement.execute();
        } catch (SQLException e) {
            log().error("Exception during table creation:", e);
            throw new DatabaseException(e);
        }
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
            try (PreparedStatement statement = SQL.Insert.statement(connection, id, record)) {
                statement.execute();
                connection.commit();
            } catch (SQLException e) {
                logTransactionException(id, e);
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
             PreparedStatement statement = SQL.SelectCommandById.statement(connection, id)) {
            final CommandStorageRecord record = readDeserializedRecord(statement, SQL.COMMAND, RECORD_DESCRIPTOR);
            return record;
        } catch (SQLException e) {
            logTransactionException(id, e);
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

    private static void logTransactionException(String commandId, SQLException e) {
        log().error("Error during transaction, command ID = " + commandId, e);
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
