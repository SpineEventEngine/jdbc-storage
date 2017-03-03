/*
 * Copyright 2017, TeamDev Ltd. All rights reserved.
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

import com.google.common.base.Optional;
import org.spine3.base.CommandId;
import org.spine3.base.CommandStatus;
import org.spine3.base.Error;
import org.spine3.base.Failure;
import org.spine3.server.command.CommandRecord;
import org.spine3.server.command.CommandStorage;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.JdbcStorageFactory;
import org.spine3.server.storage.jdbc.builder.StorageBuilder;
import org.spine3.server.storage.jdbc.command.query.CommandStorageQueryFactory;
import org.spine3.server.storage.jdbc.table.CommandTable;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;

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

    private final CommandTable table;

    protected JdbcCommandStorage(DataSourceWrapper dataSource,
                                 boolean multitenant)
            throws DatabaseException {
        super(multitenant);
        this.dataSource = dataSource;
        table = new CommandTable(dataSource);
        table.createIfNotExists();
    }

    private JdbcCommandStorage(Builder builder) throws DatabaseException {
        this(builder.getDataSource(), builder.isMultitenant());
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if the storage is closed
     * @throws DatabaseException     if an error occurs during an interaction with the DB
     */
    @Override
    public Optional<CommandRecord> read(CommandId commandId) throws DatabaseException {
        checkNotClosed();

        final String id = commandId.getUuid();
        final CommandRecord record = table.read(id);

        return Optional.fromNullable(record);
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if the storage is closed
     * @throws DatabaseException     if an error occurs during an interaction with the DB
     */
    @Override
    public Iterator<CommandRecord> read(CommandStatus status) {
        checkNotNull(status);
        final Iterator<CommandRecord> iterator = table.readByStatus(status);
        return iterator;
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if the storage is closed
     * @throws DatabaseException     if an error occurs during an interaction with the DB
     */
    @Override
    public void write(CommandId commandId, CommandRecord record) throws DatabaseException {
        checkNotDefault(commandId);
        checkNotDefault(record);
        checkNotClosed();

        final String id = commandId.getUuid();
        table.write(id, record);
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

        final String id = commandId.getUuid();
        table.setOkStatus(id);
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

        final String id = commandId.getUuid();
        table.setError(id, error);
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

        final String id = commandId.getUuid();
        table.setFailure(id, failure);
    }

    @Override
    public void close() throws DatabaseException {
        try {
            super.close();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        dataSource.close();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder extends StorageBuilder<Builder,
                                                       JdbcCommandStorage,
                                                       CommandStorageQueryFactory> {

        private Builder() {
            super();
        }

        @Override
        protected Builder getThis() {
            return this;
        }

        @Override
        public JdbcCommandStorage doBuild() throws DatabaseException {
            return new JdbcCommandStorage(this);
        }
    }
}
