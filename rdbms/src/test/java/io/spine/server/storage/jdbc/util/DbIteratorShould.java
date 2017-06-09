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

package io.spine.server.storage.jdbc.util;

import com.google.protobuf.Any;
import org.junit.Test;
import io.spine.server.storage.jdbc.DatabaseException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.NoSuchElementException;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Dmytro Dashenkov
 */
public class DbIteratorShould {

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    // Need to call a constructor and fail in it
    @Test(expected = DatabaseException.class)
    public void throw_DatabaseException_on_sql_execution_failure() throws SQLException {
        final PreparedStatement statement = mock(PreparedStatement.class);
        when(statement.executeQuery()).thenThrow(new SQLException("Failure!"));

        // Calls to PreparedStatement#executeQuery
        new DbIterator<>(statement, "", Any.getDescriptor());
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

    @Test(expected = IllegalStateException.class)
    public void throw_if_not_called_hasNext_before_next() {
        final DbIterator iterator = newIterator();
        iterator.next();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void not_support_removal() {
        final DbIterator iterator = newIterator();
        iterator.remove();
    }

    @Test(expected = NoSuchElementException.class)
    public void throw_if_trying_to_get_absent_element() {
        final DbIterator iterator = newIterator();

        // Ignore that the element is absent
        iterator.hasNext();

        // Get an error
        iterator.next();
    }

    private static DbIterator newIterator() {
        final ResultSet resultSet = mock(ResultSet.class);
        final PreparedStatement statement = mock(PreparedStatement.class);
        try {
            when(resultSet.next()).thenReturn(false);
            when(statement.executeQuery()).thenReturn(resultSet);
        } catch (SQLException e) {
            fail(e.getMessage());
        }

        final DbIterator iterator = new DbIterator<>(statement, "", Any.getDescriptor());
        return iterator;
    }

    private static DbIterator faultyResultIterator() {
        final ResultSet resultSet = mock(ResultSet.class);
        final PreparedStatement statement = mock(PreparedStatement.class);
        try {
            final Exception failure = new SQLException("Faulty Result in action");
            when(resultSet.next()).thenThrow(failure);
            doThrow(failure).when(resultSet)
                            .close();
            when(statement.executeQuery()).thenReturn(resultSet);
        } catch (SQLException e) {
            fail(e.getMessage());
        }

        final DbIterator iterator = new DbIterator<>(statement, "", Any.getDescriptor());
        return iterator;
    }

    private static DbIterator sneakyResultIterator() {
        final ResultSet resultSet = mock(ResultSet.class);
        final PreparedStatement statement = mock(PreparedStatement.class);
        try {
            when(resultSet.next())
                    .thenReturn(true);
            when(resultSet.getBytes(any(String.class)))
                    .thenThrow(
                            new SQLException("Read i snot allowed; I'm sneaky"));
            when(statement.executeQuery())
                    .thenReturn(resultSet);
        } catch (SQLException e) {
            fail(e.getMessage());
        }

        final DbIterator iterator = new DbIterator<>(statement, "", Any.getDescriptor());
        return iterator;
    }
}
