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

import io.spine.server.storage.jdbc.ConnectionWrapper;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.IdColumn;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * @author Dmytro Grankin
 */
public class SelectEventRecordsByIdShould {

    @SuppressWarnings("unchecked")
    @Test(expected = DatabaseException.class)
    public void handle_invalid_fetch_size() throws SQLException {
        final IdColumn idColumn = mock(IdColumn.class);
        final SelectEventRecordsById query = spy(SelectEventRecordsById.newBuilder("table")
                                                                       .setIdColumn(idColumn)
                                                                       .build());
        final ConnectionWrapper connection = mock(ConnectionWrapper.class);
        final PreparedStatement statement = mock(PreparedStatement.class);
        doReturn(connection).when(query)
                            .getConnection(anyBoolean());
        doReturn(statement).when(query)
                           .prepareStatement(connection);

        final int invalidFetchSize = -1;
        doThrow(SQLException.class).when(statement)
                                   .setFetchSize(invalidFetchSize);
        query.execute(invalidFetchSize);
    }
}
