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

package io.spine.server.storage.jdbc.type;

import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.Sql;
import io.spine.test.Tests;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Dmytro Dashenkov
 */
public class JdbcColumnTypeShould {

    private final JdbcColumnType<String, ?> columnType = JdbcColumnTypes.stringType();

    @Test
    public void set_null_to_prepared_statement() throws SQLException {
        final int index = 42;
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        columnType.setNull(preparedStatement, index);
        verify(preparedStatement).setNull(eq(index),
                                          eq(Sql.Type.VARCHAR_999.getSqlTypeIntIdentifier()));
    }

    @Test(expected = DatabaseException.class)
    public void throw_DatabaseException_on_SQLException() throws SQLException {
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        doThrow(DatabaseException.class).when(preparedStatement).setNull(anyInt(), anyInt());
        columnType.setNull(preparedStatement, 1);
    }

    @Test(expected = NullPointerException.class)
    public void check_converted_value_to_be_nonnull() {
        columnType.convertColumnValue(Tests.<String>nullRef());
    }
}
