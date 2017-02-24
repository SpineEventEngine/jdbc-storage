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

package org.spine3.server.storage.jdbc.query;

import org.junit.Test;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Andrey Lavrov
 */
public class WriteRecordQueryShould {

    @Test
    public void handle_database_exception() throws SQLException {
        final WriteRecordQuery query = Given.getWriteRecordQueryMock();
        try {
            query.execute();
            fail();
        } catch (DatabaseException expected) {
            verify(Given.getLoggerMock()).error(anyString(), any(SQLException.class));
        }
    }

    @Test(expected = DatabaseException.class)
    public void handle_SQL_exception_when_preparing_query() throws SQLException {
        final WriteRecordQuery query = Given.getWriteRecordQueryMock();
        final ConnectionWrapper connectionMock = mock(ConnectionWrapper.class);
        final PreparedStatement statementMock = mock(PreparedStatement.class);

        when(connectionMock.prepareStatement(anyString())).thenReturn(statementMock);
        doThrow(SQLException.class).when(statementMock).setBytes(anyInt(), any(byte[].class));

        query.prepareStatement(connectionMock);
    }
}
