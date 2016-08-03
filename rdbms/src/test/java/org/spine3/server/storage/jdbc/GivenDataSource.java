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

package org.spine3.server.storage.jdbc;

import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.mockito.Mockito.*;

public class GivenDataSource {

    @SuppressWarnings("ThrowableInstanceNeverThrown")
    private static final SQLException EXCEPTION = new SQLException("");

    private GivenDataSource() {}

    public static DataSourceWrapper whichThrowsExceptionOnSettingStatementParam() throws SQLException {
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ConnectionWrapper connection = mock(ConnectionWrapper.class);
        final DataSourceWrapper dataSource = mock(DataSourceWrapper.class);

        when(dataSource.getConnection(anyBoolean())).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);

        doThrow(EXCEPTION).when(preparedStatement).setInt(anyInt(), anyInt());
        doThrow(EXCEPTION).when(preparedStatement).setLong(anyInt(), anyLong());
        doThrow(EXCEPTION).when(preparedStatement).setString(anyInt(), anyString());

        return dataSource;
    }

    public static DataSourceWrapper whichThrowsExceptionOnExecuteStatement() throws SQLException {
        final DataSourceWrapper dataSource = mock(DataSourceWrapper.class);
        final PreparedStatement statement = mock(PreparedStatement.class);
        final ConnectionWrapper connection = mock(ConnectionWrapper.class);

        when(dataSource.getConnection(anyBoolean())).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);

        doThrow(EXCEPTION).when(statement).execute();
        doThrow(EXCEPTION).when(statement).executeQuery();

        return dataSource;
    }

    public static ClosableDataSource whichIsAutoCloseable(){
        return mock(ClosableDataSource.class);
    }

    @SuppressWarnings("InterfaceNeverImplemented")
    public interface ClosableDataSource extends DataSource, AutoCloseable {
    }
}
