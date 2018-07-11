/*
 * Copyright 2018, TeamDev. All rights reserved.
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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Dmytro Dashenkov
 */
@DisplayName("ConnectionWrapper should")
class ConnectionWrapperTest {

    @SuppressWarnings("ObjectEquality")
    @Test
    @DisplayName("store and retrieve connection")
    void storeAndRetrieveConnection() {
        final Connection connection = mockConnection();
        final ConnectionWrapper wrapper = ConnectionWrapper.wrap(connection);
        final Connection stored = wrapper.get();
        assertTrue(stored == connection); // Same object.
    }

    @Test
    @DisplayName("throw DatabaseException on SQLException on commit")
    void handleExceptionOnCommit() {
        final Connection connection = mockConnection();
        final ConnectionWrapper wrapper = ConnectionWrapper.wrap(connection);
        assertThrows(DatabaseException.class, wrapper::commit);
    }

    @Test
    @DisplayName("throw DatabaseException on SQLException on rollback")
    void handleExceptionOnRollback() {
        final Connection connection = mockConnection();
        final ConnectionWrapper wrapper = ConnectionWrapper.wrap(connection);
        assertThrows(DatabaseException.class, wrapper::rollback);
    }

    @Test
    @DisplayName("throw DatabaseException on SQLException on close")
    void handleExceptionOnClose() {
        final Connection connection = mockConnection();
        final ConnectionWrapper wrapper = ConnectionWrapper.wrap(connection);
        assertThrows(DatabaseException.class, wrapper::close);
    }

    @SuppressWarnings("JDBCPrepareStatementWithNonConstantString") // OK for a mock setup.
    @Test
    @DisplayName("throw DatabaseException on SQLException when preparing statement")
    void handleExceptionOnPrepareStatement()
            throws SQLException {
        final Connection connection = mockConnection();
        when(connection.prepareStatement(anyString())).thenThrow(SQLException.class);
        final ConnectionWrapper wrapper = ConnectionWrapper.wrap(connection);
        assertThrows(DatabaseException.class,
                     () -> wrapper.prepareStatement("SOME SQL STATEMENT."));
    }

    @Test
    @DisplayName("rollback transaction successfully")
    void rollbackTransaction() throws SQLException {
        final Connection connection = mock(Connection.class);
        final ConnectionWrapper wrapper = ConnectionWrapper.wrap(connection);
        wrapper.rollback();
        verify(connection).rollback();
    }

    private static Connection mockConnection() {
        final Connection connection = mock(Connection.class);
        @SuppressWarnings("NewExceptionWithoutArguments")
        final Exception exception = new SQLException();
        try {
            doThrow(exception).when(connection).commit();
            doThrow(exception).when(connection).rollback();
            doThrow(exception).when(connection).close();
        } catch (SQLException e) {
            fail("Didn't want to catch that.");
        }
        return connection;
    }
}