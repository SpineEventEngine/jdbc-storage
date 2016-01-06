/*
 * Copyright 2015, TeamDev Ltd. All rights reserved.
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

package org.spine3.server.storage.hsqldb;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.google.common.base.Throwables.propagate;

/**
 * The facade for operations with HyperSQL Database.
 * Uses <a href="https://github.com/brettwooldridge/HikariCP">HikariCP</a connection pool</a>.
 *
 * @author Alexander Litus
 */
class HsqlDb implements AutoCloseable {

    private final HikariDataSource dataSource;

    /**
     * Creates a new instance with the specified configuration.
     *
     * <p>Please see {@link HsqlStorageFactory#newInstance(HikariConfig)} for more info.
     *
     * @param config the config used to create {@link HikariDataSource}.
     * @see
     */
    static HsqlDb newInstance(HikariConfig config) {
        return new HsqlDb(config);
    }

    private HsqlDb(HikariConfig config) {
        dataSource = new HikariDataSource(config);
    }

    /**
     * Retrieves a wrapped connection with the given auto commit mode.
     *
     * @throws RuntimeException if a database access error occurs
     * @see Connection#setAutoCommit(boolean)
     */
    ConnectionWrapper getConnection(boolean autoCommit) {
        try {
            final Connection connection = dataSource.getConnection();
            connection.setAutoCommit(autoCommit);
            final ConnectionWrapper wrapper = ConnectionWrapper.wrap(connection);
            return wrapper;
        } catch (SQLException e) {
            throw propagate(e);
        }
    }

    /**
     * Executes the SQL statement in {@link PreparedStatement} object.
     *
     * @param sql the SQL statement to execute
     * @throws RuntimeException if a database access error occurs
     * @see PreparedStatement#execute(String)
     */
    void execute(String sql) {
        //noinspection JDBCPrepareStatementWithNonConstantString
        try (ConnectionWrapper connection = getConnection(true);
             final PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.execute();
        } catch (SQLException e) {
            propagate(e);
        }
    }

    @Override
    public void close() {
        dataSource.close();
    }
}
