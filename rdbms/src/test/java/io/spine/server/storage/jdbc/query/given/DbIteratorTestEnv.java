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

import io.spine.server.storage.jdbc.query.ColumnReader;
import io.spine.server.storage.jdbc.query.DbIterator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
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
        ResultSet resultSet = baseResultSetMock();
        try {
            when(resultSet.next()).thenReturn(false);
        } catch (SQLException e) {
            fail(e.getMessage());
        }

        DbIterator iterator = anIterator(resultSet);
        return iterator;
    }

    public static DbIterator faultyResultIterator() {
        ResultSet resultSet = baseResultSetMock();
        try {
            Exception failure = new SQLException("Faulty Result in action");
            when(resultSet.next()).thenThrow(failure);
            doThrow(failure).when(resultSet)
                            .close();
        } catch (SQLException e) {
            fail(e.getMessage());
        }

        DbIterator iterator = anIterator(resultSet);
        return iterator;
    }

    public static DbIterator sneakyResultIterator() {
        ResultSet resultSet = baseResultSetMock();
        DbIterator iterator = throwingIterator(resultSet);
        try {
            when(resultSet.next()).thenReturn(true);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        return iterator;
    }

    public static DbIterator nonEmptyIterator() {
        ResultSet resultSet = baseResultSetMock();
        try {
            when(resultSet.next()).thenReturn(true);
            when(resultSet.getBytes(any(String.class))).thenReturn(EMPTY_BYTES);
        } catch (SQLException e) {
            fail(e.getMessage());
        }

        DbIterator iterator = anIterator(resultSet);
        return iterator;
    }

    private static ResultSet baseResultSetMock() {
        ResultSet resultSet = mock(ResultSet.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        Connection connection = mock(Connection.class);
        try {
            when(statement.executeQuery()).thenReturn(resultSet);
            when(resultSet.getStatement()).thenReturn(statement);
            when(statement.getConnection()).thenReturn(connection);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        return resultSet;
    }

    private static DbIterator anIterator(ResultSet resultSet) {
        DbIterator<ResultSet> result = DbIterator.createFor(
                resultSet,
                new ColumnReader<ResultSet>("") {
                    @Override
                    public ResultSet read(ResultSet resultSet) throws SQLException {
                        return resultSet;
                    }
                }
        );
        return result;
    }

    private static DbIterator throwingIterator(ResultSet resultSet) {
        DbIterator<ResultSet> result = DbIterator.createFor(
                resultSet,
                new ColumnReader<ResultSet>("") {
                    @Override
                    public ResultSet read(ResultSet resultSet) throws SQLException {
                        throw new SQLException("Read is not allowed; I'm sneaky");
                    }
                }
        );
        return result;
    }
}
