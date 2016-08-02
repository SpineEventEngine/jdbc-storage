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
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import static org.mockito.Mockito.*;

public class DataSourceMock { // TODO:2016-08-02:alexander.litus: refactor

    private DataSourceMock() {
    }

    public static DataSourceWrapper getMockDataSourceExceptionOnAnySet() throws SQLException {
        final PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
        final ConnectionWrapper connectionMock = mock(ConnectionWrapper.class);
        final DataSourceWrapper dataSourceMock = mock(DataSourceWrapper.class);

        when(dataSourceMock.getConnection(anyBoolean())).thenReturn(connectionMock);
        when(connectionMock.prepareStatement(anyString())).thenReturn(preparedStatementMock);

        doThrow(new SQLException("")).when(preparedStatementMock).setInt(anyInt(), anyInt());
        doThrow(new SQLException("")).when(preparedStatementMock).setLong(anyInt(), anyLong());
        doThrow(new SQLException("")).when(preparedStatementMock).setString(anyInt(), anyString());

        return dataSourceMock;
    }

    public static DataSourceWrapper getMockDataSourceExceptionOnAnyExecute() throws SQLException {
        final DataSourceWrapper dataSourceMock = mock(DataSourceWrapper.class);
        final PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
        final ConnectionWrapper connectionMock = mock(ConnectionWrapper.class);

        when(dataSourceMock.getConnection(anyBoolean())).thenReturn(connectionMock);
        when(connectionMock.prepareStatement(anyString())).thenReturn(preparedStatementMock);

        doThrow(new SQLException("")).when(preparedStatementMock).execute();
        doThrow(new SQLException("")).when(preparedStatementMock).executeQuery();

        return dataSourceMock;
    }

    public static DataSource getClosableDataSourceWrapper(){
        return new ClosableDataSourceMock();
    }

    @SuppressWarnings("ReturnOfNull")
    private static class ClosableDataSourceMock implements DataSource, AutoCloseable{

        @Override
        public void close() throws Exception {

        }

        @Override
        public Connection getConnection() throws SQLException {
            return null;
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            return null;
        }

        @Override
        public PrintWriter getLogWriter() throws SQLException {
            return null;
        }

        @Override
        public void setLogWriter(PrintWriter out) throws SQLException {

        }

        @Override
        public void setLoginTimeout(int seconds) throws SQLException {

        }

        @Override
        public int getLoginTimeout() throws SQLException {
            return 0;
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return null;
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return null;
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return false;
        }
    }
}
