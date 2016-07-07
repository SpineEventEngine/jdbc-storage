/*
 * Copyright 2016, TeamDev Ltd. All rights reserved.
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

package org.spine3.server.storage.jdbc.query;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import org.junit.Test;
import org.slf4j.Logger;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * @author Andrey Lavrov
 */
public class SelectByIdQueryShould {

    @Test
    public void handle_database_exception() throws SQLException {
        final Logger logger = mock(Logger.class);
        final DataSourceWrapper dataSourceMock = mock(DataSourceWrapper.class);
        final ConnectionWrapper connectionMock = mock(ConnectionWrapper.class);
        final PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
        final IdColumn<String> idColumnMock = mock(IdColumn.StringIdColumn.class);

        when(dataSourceMock.getConnection(anyBoolean())).thenReturn(connectionMock);
        when(connectionMock.prepareStatement(anyString())).thenReturn(preparedStatementMock);
        when(preparedStatementMock.executeQuery()).thenThrow(new SQLException(""));

        final SelectByIdQuery query = Given.SelectByIdQueryMockExtension.newBuilder()
                .setDataSource(dataSourceMock)
                .setLogger(logger)
                .setIdColumn(idColumnMock)
                .build();
        try {
            query.execute();
            fail();
        } catch (DatabaseException expected) {
            verify(logger).error(anyString(), any(SQLException.class));
        }
    }

    @Test
    public void return_null_if_nothing_was_read_from_db() throws SQLException {
        final Logger logger = mock(Logger.class);
        final DataSourceWrapper dataSourceMock = mock(DataSourceWrapper.class);
        final ConnectionWrapper connectionMock = mock(ConnectionWrapper.class);
        final PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
        final IdColumn<String> idColumnMock = mock(IdColumn.StringIdColumn.class);
        final ResultSet resultSetMock = mock(ResultSet.class);

        when(dataSourceMock.getConnection(anyBoolean())).thenReturn(connectionMock);
        when(connectionMock.prepareStatement(anyString())).thenReturn(preparedStatementMock);
        when(preparedStatementMock.executeQuery()).thenReturn(resultSetMock);
        when(resultSetMock.next()).thenReturn(true);
        when(resultSetMock.getBytes(anyString())).thenReturn(null);

        final SelectByIdQuery query = Given.SelectByIdQueryMockExtension.newBuilder()
                .setDataSource(dataSourceMock)
                .setLogger(logger)
                .setIdColumn(idColumnMock)
                .setMessageColumnName(anyString())
                .setMessageDescriptor(Any.getDescriptor())
                .build();
        final Message result = query.execute();
        assertNull("If nothing is read from the database the result of the query must be null", result);
    }
}
