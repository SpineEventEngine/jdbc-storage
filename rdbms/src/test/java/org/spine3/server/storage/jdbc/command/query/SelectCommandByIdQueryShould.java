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

package org.spine3.server.storage.jdbc.command.query;

import org.junit.Test;
import org.spine3.server.command.CommandRecord;
import org.spine3.server.storage.jdbc.DatabaseException;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Andrey Lavrov
 */
public class SelectCommandByIdQueryShould {

    @Test
    public void handle_sql_exception() throws SQLException {
        final SelectCommandByIdQuery query = Given.getSelectCommandByIdQueryMock();
        try {
            query.execute();
            fail();
        } catch (DatabaseException expected) {
            verify(Given.getLoggerMock()).error(anyString(), any(SQLException.class));
        }
    }

    @Test
    public void return_null_if_result_set_contains_NO_column_COMMAND() throws SQLException {
        final SelectCommandByIdQuery query = Given.getSelectCommandByIdQueryMock();
        final ResultSet resultSet = mock(ResultSet.class);
        final CommandRecord readMessage = query.readMessage(resultSet);
        assertNull(readMessage);
    }
}
