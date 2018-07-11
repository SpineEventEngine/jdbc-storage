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

package io.spine.server.storage.jdbc.query.given;

import io.spine.server.storage.jdbc.query.DbIterator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * @author Dmytro Dashenkov
 * @author Dmytro Kuzmin
 */
public class DbIteratorTestEnv {

    private static final byte[] EMPTY_BYTES = new byte[0];

    /** Prevents instantiation of this utility class. */
    private DbIteratorTestEnv() {
    }

    public static DbIterator emptyIterator() {
        final ResultSet resultSet = baseResultSetMock();
        try {
            when(resultSet.next()).thenReturn(false);
        } catch (SQLException e) {
            fail(e.getMessage());
        }

        final DbIterator iterator = new AnIterator(resultSet);
        return iterator;
    }

    public static DbIterator faultyResultIterator() {
        final ResultSet resultSet = baseResultSetMock();
        try {
            final Exception failure = new SQLException("Faulty Result in action");
            when(resultSet.next()).thenThrow(failure);
            doThrow(failure).when(resultSet)
                            .close();
        } catch (SQLException e) {
            fail(e.getMessage());
        }

        final DbIterator iterator = new AnIterator(resultSet);
        return iterator;
    }

    public static DbIterator sneakyResultIterator() {
        final ResultSet resultSet = baseResultSetMock();
        final AnIterator iterator = spy(new AnIterator(resultSet));
        try {
            when(resultSet.next()).thenReturn(true);
            when(iterator.readResult())
                    .thenThrow(new SQLException("Read is not allowed; I'm sneaky"));
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        return iterator;
    }

    public static DbIterator nonEmptyIterator() {
        final ResultSet resultSet = baseResultSetMock();
        try {
            when(resultSet.next()).thenReturn(true);
            when(resultSet.getBytes(any(String.class))).thenReturn(EMPTY_BYTES);
        } catch (SQLException e) {
            fail(e.getMessage());
        }

        final DbIterator iterator = new AnIterator(resultSet);
        return iterator;
    }

    private static ResultSet baseResultSetMock() {
        final ResultSet resultSet = mock(ResultSet.class);
        final PreparedStatement statement = mock(PreparedStatement.class);
        final Connection connection = mock(Connection.class);
        try {
            when(statement.executeQuery()).thenReturn(resultSet);
            when(resultSet.getStatement()).thenReturn(statement);
            when(statement.getConnection()).thenReturn(connection);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        return resultSet;
    }

    private static class AnIterator extends DbIterator {

        private AnIterator(ResultSet resultSet) {
            super(resultSet, "");
        }

        @Override
        public Object readResult() throws SQLException {
            return getResultSet();
        }
    }
}
