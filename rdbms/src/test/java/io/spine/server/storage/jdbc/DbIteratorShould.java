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

package io.spine.server.storage.jdbc;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import io.spine.type.TypeUrl;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.NoSuchElementException;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Dmytro Dashenkov
 */
public class DbIteratorShould {

    private static final byte[] EMPTY_BYTES = new byte[0];

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    // Need to call a constructor and fail in it
    @Test(expected = DatabaseException.class)
    public void throw_DatabaseException_on_sql_execution_failure() throws SQLException {
        final PreparedStatement statement = mock(PreparedStatement.class);
        when(statement.executeQuery()).thenThrow(new SQLException("Failure!"));

        // Calls to PreparedStatement#executeQuery
        new MessageDbIterator<>(statement, "", TypeUrl.of(Any.class));
    }

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
    public void set_fetch_size() throws SQLException {
        final ResultSet resultSet = mock(ResultSet.class);
        final MessageDbIterator<Message> iterator = new MessageDbIterator<>(resultSet, "",
                                                                            TypeUrl.of(Any.class));
        final int fetchSize = 10;
        iterator.setFetchSize(fetchSize);
        verify(resultSet).setFetchSize(fetchSize);
    }

    @Test(expected = DatabaseException.class)
    public void wrap_sql_exception_for_invalid_fetch_size() throws SQLException {
        final int invalidFetchSize = -1;
        final ResultSet resultSet = mock(ResultSet.class);
        final MessageDbIterator<Message> iterator = new MessageDbIterator<>(resultSet, "",
                                                                            TypeUrl.of(Any.class));
        doThrow(SQLException.class).when(resultSet)
                                   .setFetchSize(invalidFetchSize);
        iterator.setFetchSize(invalidFetchSize);
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

    private static DbIterator emptyIterator() {
        final ResultSet resultSet = baseResultSetMock();
        PreparedStatement statement = null;
        try {
            statement = (PreparedStatement) resultSet.getStatement();
            when(resultSet.next()).thenReturn(false);
        } catch (SQLException e) {
            fail(e.getMessage());
        }

        final DbIterator iterator = new MessageDbIterator<>(statement, "", TypeUrl.of(Any.class));
        return iterator;
    }

    private static DbIterator faultyResultIterator() {
        final ResultSet resultSet = baseResultSetMock();
        PreparedStatement statement = null;
        try {
            statement = (PreparedStatement) resultSet.getStatement();
            final Exception failure = new SQLException("Faulty Result in action");
            when(resultSet.next()).thenThrow(failure);
            doThrow(failure).when(resultSet)
                            .close();
        } catch (SQLException e) {
            fail(e.getMessage());
        }

        final DbIterator iterator = new MessageDbIterator<>(statement, "", TypeUrl.of(Any.class));
        return iterator;
    }

    private static DbIterator sneakyResultIterator() {
        final ResultSet resultSet = baseResultSetMock();
        PreparedStatement statement = null;
        try {
            statement = (PreparedStatement) resultSet.getStatement();
            when(resultSet.next()).thenReturn(true);
            when(resultSet.getBytes(any(String.class)))
                    .thenThrow(new SQLException("Read is not allowed; I'm sneaky"));
        } catch (SQLException e) {
            fail(e.getMessage());
        }

        final DbIterator iterator = new MessageDbIterator<>(statement, "", TypeUrl.of(Any.class));
        return iterator;
    }

    private static DbIterator nonEmptyIterator() {
        final ResultSet resultSet = baseResultSetMock();
        PreparedStatement statement = null;
        try {
            statement = (PreparedStatement) resultSet.getStatement();
            when(resultSet.next()).thenReturn(true);
            when(resultSet.getBytes(any(String.class))).thenReturn(EMPTY_BYTES);
        } catch (SQLException e) {
            fail(e.getMessage());
        }

        final DbIterator iterator = new MessageDbIterator<>(statement, "", TypeUrl.of(Any.class));
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
}
