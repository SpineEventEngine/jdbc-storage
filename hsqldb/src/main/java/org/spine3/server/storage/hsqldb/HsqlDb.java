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

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.google.common.base.Throwables.propagate;

/**
 * The facade for operations with HyperSQL Database.
 *
 * @author Alexander Litus
 */
class HsqlDb implements AutoCloseable {

    private static final String JDBC_DRIVER_NAME = "org.hsqldb.jdbc.JDBCDriver";

    private final ComboPooledDataSource dataSource;

    /**
     * Creates a new instance.
     *
     * @param dbUrl    the database URL of the form {@code jdbc:subprotocol:subname},
     *                 e.g. {@code jdbc:hsqldb:hsql://localhost:9001/dbname;ifexists=true} or
     *                 {@code jdbc:hsqldb:mem:inmemorydb} for in-memory database
     * @param username the user of the database on whose behalf the connections are being made
     * @param password the user's password
     */
    static HsqlDb newInstance(String dbUrl, String username, String password) {
        return new HsqlDb(dbUrl, username, password);
    }

    /**
     * Creates a new instance. Empty {@code username} and {@code password} are used.
     * May be used for in-memory databases in tests.
     *
     * @param dbUrl the database URL of the form {@code jdbc:subprotocol:subname}, e.g. {@code jdbc:hsqldb:mem:inmemorydb}
     */
    static HsqlDb newInstance(String dbUrl) {
        return new HsqlDb(dbUrl, "", "");
    }

    private HsqlDb(String dbUrl, String username, String password) {
        dataSource = new ComboPooledDataSource();
        try {
            dataSource.setDriverClass(JDBC_DRIVER_NAME);
        } catch (PropertyVetoException e) {
            propagate(e);
        }
        dataSource.setJdbcUrl(dbUrl);
        dataSource.setUser(username);
        dataSource.setPassword(password);

        dataSource.setMaxStatements(200);
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
