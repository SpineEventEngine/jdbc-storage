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

package org.spine3.server.storage.jdbc.command.query;

import org.junit.Test;
import org.slf4j.Logger;
import org.spine3.base.CommandStatus;
import org.spine3.server.storage.CommandStorageRecord;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

/**
 * @author Andrey Lavrov
 */
public class WriteCommandRecordQueryShould {

    @Test
    public void handle_sql_exception() throws SQLException {
        final Logger logger = mock(Logger.class);
        final DataSourceWrapper dataSourceMock = mock(DataSourceWrapper.class);
        final PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
        final ConnectionWrapper connectionMock = mock(ConnectionWrapper.class);
        final IdColumn<String> idColumnMock = mock(IdColumn.StringIdColumn.class);

        when(dataSourceMock.getConnection(anyBoolean())).thenReturn(connectionMock);
        when(connectionMock.prepareStatement(anyString())).thenReturn(preparedStatementMock);
        doThrow(new SQLException("")).when(preparedStatementMock).setString(anyInt(), anyString());

        final WriteCommandRecordQuery query = InsertCommandQuery.newBuilder()
                .setDataSource(dataSourceMock)
                .setLogger(logger)
                .setIdColumn(idColumnMock)
                .setRecord(CommandStorageRecord.getDefaultInstance())
                .setStatus(CommandStatus.UNDEFINED)
                .build();
        try {
            query.execute();
            fail();
        } catch (DatabaseException expected) {
            verify(logger).error(anyString(), any(SQLException.class));
        }
    }
}
