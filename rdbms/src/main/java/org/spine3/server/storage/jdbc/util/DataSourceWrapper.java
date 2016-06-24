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

package org.spine3.server.storage.jdbc.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.Internal;
import org.spine3.server.storage.jdbc.DatabaseException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * The base class for {@link DataSource} wrappers.
 *
 * @author Alexander Litus
 */
@Internal
public abstract class DataSourceWrapper implements AutoCloseable {

    /**
     * Returns the implementation of {@link DataSource}.
     */
    public abstract DataSource getDataSource();

    /**
     * Retrieves a wrapped connection with the given auto commit mode.
     *
     * @throws DatabaseException if an error occurs during an interaction with the DB
     * @see Connection#setAutoCommit(boolean)
     */
    public ConnectionWrapper getConnection(boolean autoCommit) throws DatabaseException {
        try {
            final DataSource dataSource = getDataSource();
            final Connection connection = dataSource.getConnection();
            connection.setAutoCommit(autoCommit);
            final ConnectionWrapper wrapper = ConnectionWrapper.wrap(connection);
            return wrapper;
        } catch (SQLException e) {
            log().error("Failed to get connection.", e);
            throw new DatabaseException(e);
        }
    }

    @Override
    public abstract void close() throws DatabaseException;

    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(DataSourceWrapper.class);
    }
}
