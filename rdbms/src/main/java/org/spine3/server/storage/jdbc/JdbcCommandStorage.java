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
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.base.CommandId;
import org.spine3.base.CommandStatus;
import org.spine3.base.Error;
import org.spine3.base.Failure;
import org.spine3.server.storage.CommandStorage;
import org.spine3.server.storage.CommandStorageRecord;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.validate.Validate;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.server.storage.jdbc.util.Serializer.deserializeMessage;
import static org.spine3.server.storage.jdbc.util.Serializer.serialize;
import static org.spine3.validate.Validate.checkNotDefault;

/**
 * The implementation of the command storage based on the RDBMS.
 *
 * @see JdbcStorageFactory
 * @author Alexander Litus
 */
@SuppressWarnings("UtilityClass")
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

    private static final Descriptor COMMAND_RECORD_DESCRIPTOR = CommandStorageRecord.getDescriptor();

    /**
     * Is command status OK column name.
     */
    private static final String IS_STATUS_OK_COL = "status_ok";

    /**
     * Command error column name.
     */
    private static final String ERROR_COL = "error";

    private static final Descriptor ERROR_DESCRIPTOR = Error.getDescriptor();

    /**
     * Command failure column name.
     */
    private static final String FAILURE_COL = "failure";

    private static final Descriptor FAILURE_DESCRIPTOR = Failure.getDescriptor();

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
    @SuppressWarnings("RefusedBequest") // the method from the superclass throws an UnsupportedOperationException
    public CommandStorageRecord read(CommandId commandId) throws DatabaseException {
        checkNotClosed();

        final SelectCommandByIdQuery query = new SelectCommandByIdQuery();
        final CommandStorageRecord record = query.execute(commandId);
        if (record == null) {
            return CommandStorageRecord.getDefaultInstance();
        }
        return record;
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if the storage is closed
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    public void write(CommandId commandId, CommandStorageRecord record) throws DatabaseException {
        checkNotDefault(commandId);
        checkNotDefault(record);
        checkNotClosed();

        if (containsRecord(commandId)) {
            new UpdateCommandQuery(record).execute(commandId);
        } else {
            new InsertCommandQuery(record).execute(commandId);
        }
    }

    private boolean containsRecord(CommandId commandId) {
        final CommandStorageRecord record = read(commandId);
        final boolean contains = Validate.isNotDefault(record);
        return contains;
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if the storage is closed
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    public void setOkStatus(CommandId commandId) throws DatabaseException {
        checkNotNull(commandId);
        checkNotClosed();

        new SetOkStatusQuery().execute(commandId);
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if the storage is closed
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    public void updateStatus(CommandId commandId, Error error) throws DatabaseException {
        checkNotNull(commandId);
        checkNotNull(error);
        checkNotClosed();

        new SetErrorQuery(error).execute(commandId);
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if the storage is closed
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    @Override
    public void updateStatus(CommandId commandId, Failure failure) throws DatabaseException {
        checkNotNull(commandId);
        checkNotNull(failure);
        checkNotClosed();

        new SetFailureQuery(failure).execute(commandId);
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

    private static class CreateTableIfDoesNotExistQuery {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String CREATE_TABLE_QUERY =
                "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (" +
                    ID_COL + " VARCHAR(512), " +
                    COMMAND_COL + " BLOB, " +
                    IS_STATUS_OK_COL + " BOOLEAN, " +
                    ERROR_COL + " BLOB, " +
                    FAILURE_COL + " BLOB, " +
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

    private abstract class WriteQuery {

        @SuppressWarnings("TypeMayBeWeakened")
        protected void execute(CommandId commandId) {
            final String id = commandId.getUuid();
            try (ConnectionWrapper connection = dataSource.getConnection(false)) {
                try (PreparedStatement statement = statement(connection, id)) {
                    statement.execute();
                    connection.commit();
                } catch (SQLException e) {
                    log().error("Error while writing, command ID = " + id, e);
                    connection.rollback();
                    throw new DatabaseException(e);
                }
            }
        }

        protected abstract PreparedStatement statement(ConnectionWrapper connection, String commandId);
    }

    private class WriteRecordQuery<M extends Message> extends WriteQuery {

        private final byte[] recordBytes;
        private final String query;
        private final int idIndexInQuery;
        private final int recordIndexInQuery;

        protected WriteRecordQuery(M record, String query, int idIndexInQuery, int recordIndexInQuery) {
            this.recordBytes = serialize(record);
            this.query = query;
            this.idIndexInQuery = idIndexInQuery;
            this.recordIndexInQuery = recordIndexInQuery;
        }

        @Override
        protected PreparedStatement statement(ConnectionWrapper connection, String commandId) {
            try {
                final PreparedStatement statement = connection.prepareStatement(query);
                statement.setString(idIndexInQuery, commandId);
                statement.setBytes(recordIndexInQuery, recordBytes);
                return statement;
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }
    }

    private class InsertCommandQuery extends WriteRecordQuery<CommandStorageRecord> {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String INSERT_QUERY =
                "INSERT INTO " + TABLE_NAME + " (" +
                    ID_COL + ", " +
                    COMMAND_COL +
                ") VALUES (?, ?);";

        private static final int ID_INDEX_IN_QUERY = 1;
        private static final int RECORD_INDEX_IN_QUERY = 2;

        protected InsertCommandQuery(CommandStorageRecord record) {
            super(record, INSERT_QUERY, ID_INDEX_IN_QUERY, RECORD_INDEX_IN_QUERY);
        }
    }

    private class UpdateCommandQuery extends WriteRecordQuery<CommandStorageRecord> {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String UPDATE_QUERY =
                "UPDATE " + TABLE_NAME +
                " SET " + COMMAND_COL + " = ? " +
                " WHERE " + ID_COL + " = ?;";

        private static final int RECORD_INDEX_IN_QUERY = 1;
        private static final int ID_INDEX_IN_QUERY = 2;

        private UpdateCommandQuery(CommandStorageRecord record) {
            super(record, UPDATE_QUERY, ID_INDEX_IN_QUERY, RECORD_INDEX_IN_QUERY);
        }
    }

    private class SetOkStatusQuery extends WriteQuery {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String SET_OK_STATUS_QUERY =
                "UPDATE " + TABLE_NAME +
                " SET " + IS_STATUS_OK_COL + " = true " +
                " WHERE " + ID_COL + " = ? ;";

        @Override
        protected PreparedStatement statement(ConnectionWrapper connection, String commandId) {
            final PreparedStatement statement = connection.prepareStatement(SET_OK_STATUS_QUERY);
            try {
                statement.setString(1, commandId);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
            return statement;
        }
    }

    private class SetErrorQuery extends WriteRecordQuery<Error> {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String SET_ERROR_QUERY =
                "UPDATE " + TABLE_NAME +
                " SET " +
                    IS_STATUS_OK_COL + " = false, " +
                    ERROR_COL + " = ? " +
                " WHERE " + ID_COL + " = ? ;";

        private static final int ERROR_INDEX_IN_QUERY = 1;
        private static final int ID_INDEX_IN_QUERY = 2;

        protected SetErrorQuery(Error error) {
            super(error, SET_ERROR_QUERY, ID_INDEX_IN_QUERY, ERROR_INDEX_IN_QUERY);
        }
    }

    private class SetFailureQuery extends WriteRecordQuery<Failure> {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String SET_FAILURE_QUERY =
                "UPDATE " + TABLE_NAME +
                " SET " +
                    IS_STATUS_OK_COL + " = false, " +
                    FAILURE_COL + " = ? " +
                " WHERE " + ID_COL + " = ? ;";

        private static final int FAILURE_INDEX_IN_QUERY = 1;
        private static final int ID_INDEX_IN_QUERY = 2;

        protected SetFailureQuery(Failure failure) {
            super(failure, SET_FAILURE_QUERY, ID_INDEX_IN_QUERY, FAILURE_INDEX_IN_QUERY);
        }
    }

    private class SelectCommandByIdQuery {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String SELECT_QUERY =
                "SELECT * FROM " + TABLE_NAME +
                " WHERE " + ID_COL + " = ?;";

        @Nullable
        @SuppressWarnings("TypeMayBeWeakened")
        private CommandStorageRecord execute(CommandId commandId) {
            try (ConnectionWrapper connection = dataSource.getConnection(true);
                 PreparedStatement statement = statement(connection, commandId.getUuid());
                 ResultSet resultSet = statement.executeQuery()) {
                final CommandStorageRecord record = readRecord(resultSet);
                return record;
            } catch (SQLException e) {
                log().error("Error while reading command record, command ID = " + commandId, e);
                throw new DatabaseException(e);
            }
        }

        @Nullable
        private CommandStorageRecord readRecord(ResultSet resultSet) throws SQLException {
            if (!resultSet.next()) {
                return null;
            }
            final byte[] commandRecordBytes = resultSet.getBytes(COMMAND_COL);
            final CommandStorageRecord record = deserializeMessage(commandRecordBytes, COMMAND_RECORD_DESCRIPTOR);
            final CommandStorageRecord.Builder builder = record.toBuilder();
            final boolean isStatusOk = resultSet.getBoolean(IS_STATUS_OK_COL);
            if (isStatusOk) {
                return builder.setStatus(CommandStatus.OK).build();
            }
            final byte[] errorBytes = resultSet.getBytes(ERROR_COL);
            if (errorBytes != null) {
                final Error error = deserializeMessage(errorBytes, ERROR_DESCRIPTOR);
                return builder.setError(error)
                        .setStatus(CommandStatus.ERROR)
                        .build();
            }
            final byte[] failureBytes = resultSet.getBytes(FAILURE_COL);
            if (failureBytes != null) {
                final Failure failure = deserializeMessage(failureBytes, FAILURE_DESCRIPTOR);
                return builder.setFailure(failure)
                        .setStatus(CommandStatus.FAILURE)
                        .build();
            }
            return builder.build();
        }

        private PreparedStatement statement(ConnectionWrapper connection, String id) {
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
