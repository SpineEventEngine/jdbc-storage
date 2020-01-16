/*
 * Copyright 2020, TeamDev. All rights reserved.
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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * The wrapper for {@link Connection} instances.
 */
@Internal
public final class ConnectionWrapper implements AutoCloseable {

    private final Connection connection;

    /**
     * Creates a new instance wrapping the {@code connection}.
     *
     * @param connection the connection to wrap
     */
    static ConnectionWrapper wrap(Connection connection) {
        return new ConnectionWrapper(connection);
    }

    private ConnectionWrapper(Connection connection) {
        this.connection = connection;
    }

    /** Returns the wrapped connection object. */
    public Connection get() {
        return connection;
    }

    /**
     * Wraps {@link Connection#prepareStatement(String)} method.
     *
     * @throws DatabaseException
     *         if SQLException occurs
     */
    public PreparedStatement prepareStatement(String sql) {
        try {
            @SuppressWarnings("JDBCPrepareStatementWithNonConstantString")
            // Preparing a statement provided by a StorageQuery.
            PreparedStatement statement = connection.prepareStatement(sql);
            return statement;
        } catch (SQLException e) {
            throw new DatabaseException("Error executing statement " + sql, e);
        }
    }

    /**
     * Wraps {@link Connection#commit()} method.
     *
     * @throws DatabaseException
     *         if SQLException occurs
     */
    public void commit() {
        try {
            connection.commit();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    /**
     * Wraps {@link Connection#rollback()} method.
     *
     * @throws DatabaseException
     *         if SQLException occurs
     */
    public void rollback() {
        try {
            connection.rollback();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    /**
     * Wraps {@link Connection#close()} method.
     *
     * @throws DatabaseException
     *         if SQLException occurs
     */
    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }
}
