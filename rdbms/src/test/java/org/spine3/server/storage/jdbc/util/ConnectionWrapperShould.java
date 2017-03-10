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

package org.spine3.server.storage.jdbc.util;

import org.junit.Test;
import org.spine3.server.storage.jdbc.DatabaseException;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Dmytro Dashenkov
 */
public class ConnectionWrapperShould {

    @SuppressWarnings("ObjectEquality")
    @Test
    public void store_and_retrieve_connection() {
        final Connection connection = mockConnection();
        final ConnectionWrapper wrapper = ConnectionWrapper.wrap(connection);
        final Connection stored = wrapper.get();
        assertTrue(stored == connection); // Same object
    }

    @Test(expected = DatabaseException.class)
    public void throw_DatabaseException_on_SQLException_on_commit() {
        final Connection connection = mockConnection();
        final ConnectionWrapper wrapper = ConnectionWrapper.wrap(connection);
        wrapper.commit();
    }

    @Test(expected = DatabaseException.class)
    public void throw_DatabaseException_on_SQLException_on_rollback() {
        final Connection connection = mockConnection();
        final ConnectionWrapper wrapper = ConnectionWrapper.wrap(connection);
        wrapper.rollback();
    }

    @Test(expected = DatabaseException.class)
    public void throw_DatabaseException_on_SQLException_on_close() {
        final Connection connection = mockConnection();
        final ConnectionWrapper wrapper = ConnectionWrapper.wrap(connection);
        wrapper.close();
    }

    @Test
    public void rollback_transaction_successfully() throws SQLException {
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
