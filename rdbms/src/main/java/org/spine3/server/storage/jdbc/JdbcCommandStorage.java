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
import org.spine3.base.CommandStatus;
import org.spine3.base.Error;
import org.spine3.base.Failure;
import org.spine3.server.storage.CommandStorage;
import org.spine3.server.storage.CommandStorageRecord;
import org.spine3.server.storage.jdbc.util.*;
import org.spine3.server.storage.jdbc.util.IdColumn.StringIdColumn;
import org.spine3.validate.Validate;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.server.storage.jdbc.util.Serializer.deserialize;
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

    private static final StringIdColumn STRING_ID_COLUMN = new StringIdColumn();

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
        final CommandStorageRecord record = query.execute(commandId.getUuid());
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
            UpdateCommandQuery.newInstance(dataSource, commandId, record).execute();
        } else {
            InsertCommandQuery.newInstance(dataSource, commandId, record).execute();
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

        new SetOkStatusQuery(commandId).execute();
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

        SetErrorQuery.newInstance(dataSource, commandId, error).execute();
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

        SetFailureQuery.newInstance(dataSource, commandId, failure).execute();
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

    private static class InsertCommandQuery extends WriteRecordQuery<String, CommandStorageRecord> {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String INSERT_QUERY =
                "INSERT INTO " + TABLE_NAME + " (" +
                    ID_COL + ", " +
                    COMMAND_COL +
                ") VALUES (?, ?);";

        private static final int ID_INDEX_IN_QUERY = 1;
        private static final int RECORD_INDEX_IN_QUERY = 2;

        private InsertCommandQuery(Builder builder) {
            super(builder);
        }

        public static InsertCommandQuery newInstance(DataSourceWrapper dataSource, CommandId commandId, CommandStorageRecord record) {
            final String id = commandId.getUuid();
            return new Builder()
                    .setDataSource(dataSource)
                    .setId(id)
                    .setRecord(record)
                    .build();
        }

        private static class Builder extends AbstractBuilder<InsertCommandQuery, String, CommandStorageRecord> {

            @Override
            public InsertCommandQuery build() {
                setQuery(INSERT_QUERY);
                setIdIndexInQuery(ID_INDEX_IN_QUERY);
                setRecordIndexInQuery(RECORD_INDEX_IN_QUERY);
                setIdColumn(STRING_ID_COLUMN);
                return new InsertCommandQuery(this);
            }
        }

        @Override
        protected void log(SQLException exception) {
            logError(exception, "command insertion", getId());
        }
    }

    private static class UpdateCommandQuery extends WriteRecordQuery<String, CommandStorageRecord> {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String UPDATE_QUERY =
                "UPDATE " + TABLE_NAME +
                " SET " + COMMAND_COL + " = ? " +
                " WHERE " + ID_COL + " = ?;";

        private static final int RECORD_INDEX_IN_QUERY = 1;
        private static final int ID_INDEX_IN_QUERY = 2;

        protected UpdateCommandQuery(Builder builder) {
            super(builder);
        }

        private static UpdateCommandQuery newInstance(DataSourceWrapper dataSource, CommandId commandId, CommandStorageRecord record) {
            return new Builder()
                    .setDataSource(dataSource)
                    .setId(commandId.getUuid())
                    .setRecord(record)
                    .build();
        }

        private static class Builder extends AbstractBuilder<UpdateCommandQuery, String, CommandStorageRecord> {

            @Override
            public UpdateCommandQuery build() {
                setQuery(UPDATE_QUERY);
                setIdIndexInQuery(ID_INDEX_IN_QUERY);
                setRecordIndexInQuery(RECORD_INDEX_IN_QUERY);
                setIdColumn(STRING_ID_COLUMN);
                return new UpdateCommandQuery(this);
            }
        }

        @Override
        protected void log(SQLException exception) {
            logError(exception, "updating command", getId());
        }
    }

    private class SetOkStatusQuery extends WriteQuery {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String SET_OK_STATUS_QUERY =
                "UPDATE " + TABLE_NAME +
                " SET " + IS_STATUS_OK_COL + " = true " +
                " WHERE " + ID_COL + " = ? ;";

        private final CommandId commandId;

        protected SetOkStatusQuery(CommandId commandId) {
            this.commandId = commandId;
        }

        @Override
        protected DataSourceWrapper getDataSource() {
            return dataSource;
        }

        @Override
        @SuppressWarnings("RefusedBequest")
        protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
            final PreparedStatement statement = connection.prepareStatement(SET_OK_STATUS_QUERY);
            final String id = commandId.getUuid();
            try {
                statement.setString(1, id);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
            return statement;
        }

        @Override
        protected void log(SQLException exception) {
            logError(exception, "setting OK command status", commandId.getUuid());
        }
    }

    private static class SetErrorQuery extends WriteRecordQuery<String, Error> {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String SET_ERROR_QUERY =
                "UPDATE " + TABLE_NAME +
                " SET " +
                    IS_STATUS_OK_COL + " = false, " +
                    ERROR_COL + " = ? " +
                " WHERE " + ID_COL + " = ? ;";

        private static final int ERROR_INDEX_IN_QUERY = 1;
        private static final int ID_INDEX_IN_QUERY = 2;

        protected SetErrorQuery(Builder builder) {
            super(builder);
        }

        private static SetErrorQuery newInstance(DataSourceWrapper dataSource, CommandId commandId, Error error) {
            return new Builder()
                    .setDataSource(dataSource)
                    .setId(commandId.getUuid())
                    .setRecord(error)
                    .build();
        }

        private static class Builder extends AbstractBuilder<SetErrorQuery, String, Error> {

            @Override
            public SetErrorQuery build() {
                setQuery(SET_ERROR_QUERY);
                setIdIndexInQuery(ID_INDEX_IN_QUERY);
                setRecordIndexInQuery(ERROR_INDEX_IN_QUERY);
                setIdColumn(STRING_ID_COLUMN);
                return new SetErrorQuery(this);
            }
        }

        @Override
        protected void log(SQLException exception) {
            logError(exception, "setting error command status", getId());
        }
    }

    private static class SetFailureQuery extends WriteRecordQuery<String, Failure> {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String SET_FAILURE_QUERY =
                "UPDATE " + TABLE_NAME +
                " SET " +
                    IS_STATUS_OK_COL + " = false, " +
                    FAILURE_COL + " = ? " +
                " WHERE " + ID_COL + " = ? ;";

        private static final int FAILURE_INDEX_IN_QUERY = 1;
        private static final int ID_INDEX_IN_QUERY = 2;

        protected SetFailureQuery(Builder builder) {
            super(builder);
        }

        private static SetFailureQuery newInstance(DataSourceWrapper dataSource, CommandId commandId, Failure failure) {
            return new Builder()
                    .setDataSource(dataSource)
                    .setId(commandId.getUuid())
                    .setRecord(failure)
                    .build();
        }

        private static class Builder extends AbstractBuilder<SetFailureQuery, String, Failure> {

            @Override
            public SetFailureQuery build() {
                setQuery(SET_FAILURE_QUERY);
                setIdIndexInQuery(ID_INDEX_IN_QUERY);
                setRecordIndexInQuery(FAILURE_INDEX_IN_QUERY);
                setIdColumn(STRING_ID_COLUMN);
                return new SetFailureQuery(this);
            }
        }

        @Override
        protected void log(SQLException exception) {
            logError(exception, "setting failure command status", getId());
        }
    }

    private class SelectCommandByIdQuery extends SelectByIdQuery<String, CommandStorageRecord> {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String SELECT_QUERY =
                "SELECT * FROM " + TABLE_NAME +
                " WHERE " + ID_COL + " = ?;";

        protected SelectCommandByIdQuery() {
            super(SELECT_QUERY, dataSource, new StringIdColumn());
        }

        @Nullable
        @Override
        @SuppressWarnings("RefusedBequest")
        protected CommandStorageRecord readMessage(ResultSet resultSet) throws SQLException {
            final byte[] recordBytes = resultSet.getBytes(COMMAND_COL);
            if (recordBytes == null) {
                return null;
            }
            final CommandStorageRecord record = deserialize(recordBytes, COMMAND_RECORD_DESCRIPTOR);
            final CommandStorageRecord.Builder builder = record.toBuilder();
            final boolean isStatusOk = resultSet.getBoolean(IS_STATUS_OK_COL);
            if (isStatusOk) {
                return builder.setStatus(CommandStatus.OK).build();
            }
            final byte[] errorBytes = resultSet.getBytes(ERROR_COL);
            if (errorBytes != null) {
                final Error error = deserialize(errorBytes, ERROR_DESCRIPTOR);
                return builder.setError(error)
                        .setStatus(CommandStatus.ERROR)
                        .build();
            }
            final byte[] failureBytes = resultSet.getBytes(FAILURE_COL);
            if (failureBytes != null) {
                final Failure failure = deserialize(failureBytes, FAILURE_DESCRIPTOR);
                return builder.setFailure(failure)
                        .setStatus(CommandStatus.FAILURE)
                        .build();
            }
            return builder.build();
        }
    }

    private static void logError(SQLException e, String actionName, String commandId) {
        log().error("Exception during {}, command ID: {}", actionName, commandId, e);
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
