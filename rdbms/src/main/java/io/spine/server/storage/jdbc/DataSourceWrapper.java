/*
 * Copyright 2019, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc;

import io.spine.annotation.Internal;
import io.spine.logging.Logging;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Wrapper for {@link DataSource} instances.
 */
@Internal
public class DataSourceWrapper implements AutoCloseable, Logging {

    private final DataSource dataSource;

    /** Wraps custom {@link DataSource} implementation. */
    static DataSourceWrapper wrap(DataSource dataSource) {
        return new DataSourceWrapper(dataSource);
    }

    protected DataSourceWrapper(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Retrieves a wrapped connection with the given auto commit mode.
     *
     * @throws DatabaseException
     *         if an error occurs during an interaction with the DB
     * @see Connection#setAutoCommit(boolean)
     */
    public ConnectionWrapper getConnection(boolean autoCommit) throws DatabaseException {
        try {
            Connection connection = dataSource.getConnection();
            connection.setAutoCommit(autoCommit);
            ConnectionWrapper wrapper = ConnectionWrapper.wrap(connection);
            return wrapper;
        } catch (SQLException e) {
            _error("Failed to get connection.", e);
            throw new DatabaseException(e);
        }
    }

    /**
     * Closes wrapped {@link DataSource} implementation if it implements {@link AutoCloseable}.
     *
     * <p>Otherwise a warning is logged.
     *
     * @throws DatabaseException
     *         if the {@link DataSource} throws an exception
     */
    @Override
    public void close() throws DatabaseException {
        if (dataSource instanceof AutoCloseable) {
            try {
                ((AutoCloseable) dataSource).close();
            } catch (Exception e) {
                _error("Error occurred while closing DataSource ", e);
                throw new DatabaseException(e);
            }
            return;
        }
        _warn("Close method is not implemented in " + dataSource.getClass());
    }
}
