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

package org.spine3.server.storage.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.spine3.base.Identifiers.newUuid;

public class GivenDataSource {

    public static final String DEFAULT_TABLE_NAME = "test";

    /**
     * The URL prefix of an in-memory HyperSQL DB.
     */
    private static final String HSQL_IN_MEMORY_DB_URL_PREFIX = "jdbc:hsqldb:mem:";

    @SuppressWarnings("ThrowableInstanceNeverThrown")
    private static final SQLException EXCEPTION = new SQLException("");

    private GivenDataSource() {
    }

    public static DataSourceWrapper withoutSuperpowers() {
        return mock(DataSourceWrapper.class);
    }

    public static DataSourceWrapper whichThrowsExceptionOnSettingStatementParam()
            throws SQLException {
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ConnectionWrapper connection = mock(ConnectionWrapper.class);
        final DataSourceWrapper dataSource = mock(DataSourceWrapper.class);

        when(dataSource.getConnection(anyBoolean())).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);

        doThrow(EXCEPTION).when(preparedStatement)
                          .setInt(anyInt(), anyInt());
        doThrow(EXCEPTION).when(preparedStatement)
                          .setLong(anyInt(), anyLong());
        doThrow(EXCEPTION).when(preparedStatement)
                          .setString(anyInt(), anyString());

        return dataSource;
    }

    public static DataSourceWrapper whichThrowsExceptionOnExecuteStatement() throws SQLException {
        final DataSourceWrapper dataSource = mock(DataSourceWrapper.class);
        final PreparedStatement statement = mock(PreparedStatement.class);
        final ConnectionWrapper connection = mock(ConnectionWrapper.class);

        when(dataSource.getConnection(anyBoolean())).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);

        doThrow(EXCEPTION).when(statement)
                          .execute();
        doThrow(EXCEPTION).when(statement)
                          .executeQuery();

        return dataSource;
    }

    public static ClosableDataSource whichIsAutoCloseable() {
        return mock(ClosableDataSource.class);
    }

    public static DataSourceWrapper whichIsStoredInMemory(String dbName) {
        final HikariConfig config = new HikariConfig();
        final String dbUrl = prefix(dbName);
        config.setJdbcUrl(dbUrl);
        // not setting username and password is OK for in-memory database
        final DataSourceWrapper dataSource = DataSourceWrapper.wrap(new HikariDataSource(config));
        return dataSource;
    }

    public static String prefix(String dbNamePrefix) {
        return HSQL_IN_MEMORY_DB_URL_PREFIX + dbNamePrefix + newUuid();
    }

    @SuppressWarnings("InterfaceNeverImplemented")
    public interface ClosableDataSource extends DataSource, AutoCloseable {
    }
}
