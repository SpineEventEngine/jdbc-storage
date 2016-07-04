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

package org.spine3.server.storage.jdbc.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.base.CommandId;
import org.spine3.base.CommandStatus;
import org.spine3.base.Error;
import org.spine3.base.Failure;
import org.spine3.server.storage.CommandStorage;
import org.spine3.server.storage.CommandStorageRecord;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.JdbcStorageFactory;
import org.spine3.server.storage.jdbc.command.query.CommandStorageQueryFactory;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.validate.Validate;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.validate.Validate.checkNotDefault;

/**
 * The implementation of the command storage based on the RDBMS.
 *
 * @author Alexander Litus
 * @see JdbcStorageFactory
 */
@SuppressWarnings("UtilityClass")
public class JdbcCommandStorage extends CommandStorage {

    private final DataSourceWrapper dataSource;

    private final CommandStorageQueryFactory queryFactory;

    /**
     * Creates a new storage instance.
     *
     * @param dataSource            a data source to use to obtain connections
     * @param multitenant           defines is this storage multitenant
     * @param queryFactory          factory that generates queries for interaction with command table
     * @return                      a new storage instance
     * @throws DatabaseException    if an error occurs during an interaction with the DB
     */
    public static CommandStorage newInstance(DataSourceWrapper dataSource,
                                             boolean multitenant,
                                             CommandStorageQueryFactory queryFactory)
            throws DatabaseException {
        return new JdbcCommandStorage(dataSource, multitenant, queryFactory);
    }

    protected JdbcCommandStorage(DataSourceWrapper dataSource,
                               boolean multitenant,
                               CommandStorageQueryFactory queryFactory)
            throws DatabaseException {
        super(multitenant);
        this.dataSource = dataSource;
        this.queryFactory = queryFactory;
        queryFactory.setLogger(LogSingleton.INSTANCE.value);
        queryFactory.newCreateTableQuery().execute();
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

        final CommandStorageRecord record = queryFactory.newSelectCommandByIdQuery(commandId).execute();

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
        final Iterator<CommandStorageRecord> iterator = queryFactory.newSelectCommandByStatusQuery(status).execute();
        return iterator;
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
            queryFactory.newUpdateCommandQuery(commandId, record).execute();
        } else {
            queryFactory.newInsertCommandQuery(commandId, record).execute();
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

        queryFactory.newSetOkStatusQuery(commandId).execute();
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

        queryFactory.newSetErrorQuery(commandId, error).execute();
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

        queryFactory.newSetFailureQuery(commandId, failure).execute();
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

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(JdbcCommandStorage.class);
    }
}
