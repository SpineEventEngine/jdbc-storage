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

package io.spine.server.storage.jdbc.query;

import io.spine.server.storage.jdbc.DatabaseException;
import org.junit.Test;
import org.mockito.InOrder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.NoSuchElementException;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Dmytro Dashenkov
 */
public class DbIteratorShould {

    private static final byte[] EMPTY_BYTES = new byte[0];

    @Test(expected = DatabaseException.class)
    public void throw_DatabaseException_on_next_check_failure() {
        final DbIterator iterator = faultyResultIterator();
        iterator.hasNext();
    }

    @Test(expected = DatabaseException.class)
    public void throw_DatabaseException_on_close_failure() {
        final DbIterator iterator = faultyResultIterator();
        iterator.close();
    }

    @Test(expected = DatabaseException.class)
    public void throw_DatabaseException_on_read_failure() {
        final DbIterator iterator = sneakyResultIterator();
        if (iterator.hasNext()) {
            iterator.next();
        }
    }

    @Test
    public void allow_next_without_hasNext() {
        final DbIterator iterator = nonEmptyIterator();
        iterator.next();
    }

    @Test
    public void close_ResultSet() throws SQLException {
        final DbIterator iterator = nonEmptyIterator();
        final ResultSet resultSet = iterator.getResultSet();

        verify(resultSet, never()).close();

        iterator.close();
        assertClosed(iterator);
    }

    @Test
    public void close_ResultSet_if_no_more_elements_to_iterate() throws SQLException {
        final DbIterator iterator = emptyIterator();
        final ResultSet resultSet = iterator.getResultSet();

        verify(resultSet, never()).close();

        iterator.hasNext();
        assertClosed(iterator);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void not_support_removal() {
        final DbIterator iterator = emptyIterator();
        iterator.remove();
    }

    @Test(expected = NoSuchElementException.class)
    public void throw_if_trying_to_get_absent_element() {
        final DbIterator iterator = emptyIterator();

        // Ignore that the element is absent
        iterator.hasNext();

        // Get an error
        iterator.next();
    }

    @Test
    public void check_if_result_set_closed() throws SQLException {
        final DbIterator iterator = emptyIterator();
        iterator.close();

        final ResultSet resultSet = iterator.getResultSet();
        verify(resultSet).isClosed();
        verify(resultSet).close();
    }

    @Test
    public void obtain_statement_before_result_set_closed() throws SQLException {
        final DbIterator iterator = emptyIterator();
        iterator.close();

        final ResultSet resultSet = iterator.getResultSet();
        final InOrder order = inOrder(resultSet);
        order.verify(resultSet)
             .getStatement();
        order.verify(resultSet)
             .close();
    }

    @Test
    public void obtain_connection_before_statement_closed() throws SQLException {
        final DbIterator iterator = emptyIterator();
        iterator.close();

        final Statement statement = iterator.getResultSet()
                                            .getStatement();
        final InOrder order = inOrder(statement);
        order.verify(statement)
             .getConnection();
        order.verify(statement)
             .close();
    }

    private static DbIterator emptyIterator() {
        final ResultSet resultSet = baseResultSetMock();
        try {
            when(resultSet.next()).thenReturn(false);
        } catch (SQLException e) {
            fail(e.getMessage());
        }

        final DbIterator iterator = new AnIterator(resultSet);
        return iterator;
    }

    private static DbIterator faultyResultIterator() {
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

    private static DbIterator sneakyResultIterator() {
        final ResultSet resultSet = baseResultSetMock();
        final DbIterator iterator = spy(new AnIterator(resultSet));
        try {
            when(resultSet.next()).thenReturn(true);
            when(iterator.readResult())
                    .thenThrow(new SQLException("Read is not allowed; I'm sneaky"));
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        return iterator;
    }

    private static DbIterator nonEmptyIterator() {
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

    private static void assertClosed(DbIterator<?> iterator) throws SQLException {
        final ResultSet resultSet = iterator.getResultSet();
        final Statement statement = resultSet.getStatement();
        final Connection connection = statement.getConnection();
        verify(resultSet).close();
        verify(statement).close();
        verify(connection).close();
    }

    private static class AnIterator extends DbIterator {

        private AnIterator(ResultSet resultSet) {
            super(resultSet, "");
        }

        @Override
        protected Object readResult() throws SQLException {
            return getResultSet();
        }
    }
}
