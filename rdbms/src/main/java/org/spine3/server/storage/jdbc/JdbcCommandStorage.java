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
import org.spine3.server.storage.jdbc.query.tables.commands.*;
import org.spine3.server.storage.jdbc.util.*;
import org.spine3.server.storage.jdbc.util.IdColumn.StringIdColumn;
import org.spine3.validate.Validate;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.server.storage.jdbc.util.Serializer.deserialize;
import static org.spine3.validate.Validate.checkNotDefault;

/**
 * The implementation of the command storage based on the RDBMS.
 *
 * @author Alexander Litus
 * @see JdbcStorageFactory
 */
@SuppressWarnings("UtilityClass")
/* package */ class JdbcCommandStorage extends CommandStorage {

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
    private static final String COMMAND_STATUS_COL = "command_status";

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
     * @return a new storage instance
     * @throws DatabaseException if an error occurs during an interaction with the DB
     */
    /*package*/
    static CommandStorage newInstance(DataSourceWrapper dataSource, boolean multitenant) throws DatabaseException {
        return new JdbcCommandStorage(dataSource, multitenant);
    }

    private JdbcCommandStorage(DataSourceWrapper dataSource, boolean multitenant) throws DatabaseException {
        super(multitenant);
        this.dataSource = dataSource;
        CreateCommandTableIfNo
                .getBuilder()
                .setDataSource(dataSource)
                .build()
                .execute();
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if the storage is closed
     * @throws DatabaseException     if an error occurs during an interaction with the DB
     */
    @Override
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
     * @throws DatabaseException     if an error occurs during an interaction with the DB
     */
    @Override
    public Iterator<CommandStorageRecord> read(CommandStatus status) {
        checkNotNull(status);
        try (ConnectionWrapper connection = dataSource.getConnection(true)) {
            final SelectCommandByStatusQuery query = new SelectCommandByStatusQuery(status);
            final PreparedStatement statement = query.prepareStatement(connection);
            final DbIterator<CommandStorageRecord> iterator = new DbIterator<>(statement, COMMAND_COL, COMMAND_RECORD_DESCRIPTOR);
            return iterator;
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if the storage is closed
     * @throws DatabaseException     if an error occurs during an interaction with the DB
     */
    @Override
    public void write(CommandId commandId, CommandStorageRecord record) throws DatabaseException {
        checkNotDefault(commandId);
        checkNotDefault(record);
        checkNotClosed();

        if (containsRecord(commandId)) {
            UpdateCommand.getBuilder()
                    .setStatus(CommandStatus.forNumber(record.getStatusValue()))
                    .setId(commandId.getUuid())
                    .setRecord(record)
                    .setIdColumn(STRING_ID_COLUMN)
                    .setDataSource(dataSource)
                    .build()
                    .execute();
        } else {
            InsertCommand.getBuilder()
                    .setStatus(CommandStatus.forNumber(record.getStatusValue()))
                    .setId(commandId.getUuid())
                    .setRecord(record)
                    .setIdColumn(STRING_ID_COLUMN)
                    .setDataSource(dataSource)
                    .build()
                    .execute();
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
     * @throws DatabaseException     if an error occurs during an interaction with the DB
     */
    @Override
    public void setOkStatus(CommandId commandId) throws DatabaseException {
        checkNotNull(commandId);
        checkNotClosed();

        SetOkStatus.getBuilder()
                .setIdColumn(STRING_ID_COLUMN)
                .setId(commandId.getUuid())
                .setDataSource(this.dataSource)
                .build()
                .execute();
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if the storage is closed
     * @throws DatabaseException     if an error occurs during an interaction with the DB
     */
    @Override
    public void updateStatus(CommandId commandId, Error error) throws DatabaseException {
        checkNotNull(commandId);
        checkNotNull(error);
        checkNotClosed();

        SetError.getBuilder()
                .setRecord(error)
                .setId(commandId.getUuid())
                .setIdColumn(STRING_ID_COLUMN)
                .setDataSource(dataSource)
                .build()
                .execute();
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if the storage is closed
     * @throws DatabaseException     if an error occurs during an interaction with the DB
     */
    @Override
    public void updateStatus(CommandId commandId, Failure failure) throws DatabaseException {
        checkNotNull(commandId);
        checkNotNull(failure);
        checkNotClosed();

        SetFailure.getBuilder()
                .setRecord(failure)
                .setId(commandId.getUuid())
                .setIdColumn(STRING_ID_COLUMN)
                .setDataSource(dataSource)
                .build()
                .execute();
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

    private class SelectCommandByIdQuery extends SelectByIdQuery<String, CommandStorageRecord> {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String SELECT_QUERY =
                "SELECT * FROM " + TABLE_NAME +
                " WHERE " + ID_COL + " = ?;";

        private SelectCommandByIdQuery() {
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
            final String status = resultSet.getString(COMMAND_STATUS_COL);
            if (status.equals(CommandStatus.forNumber(CommandStatus.OK_VALUE).name())) {
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

    private static void log(SQLException e, String actionName, String commandId) {
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

    private static class SelectCommandByStatusQuery extends SelectByStatusQuery {

        @SuppressWarnings("DuplicateStringLiteralInspection")
        private static final String SELECT_BY_STATUS_QUERY =
                "SELECT " +  COMMAND_COL + " FROM " + TABLE_NAME +
                " WHERE " + COMMAND_STATUS_COL + " = ?;";

        private SelectCommandByStatusQuery(CommandStatus status) {
            super(SELECT_BY_STATUS_QUERY, status);
        }
    }
}
