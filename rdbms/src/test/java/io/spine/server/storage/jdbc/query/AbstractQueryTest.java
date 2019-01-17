/*
 * Copyright 2019, TeamDev. All rights reserved.
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

import com.querydsl.sql.AbstractSQLQueryFactory;
import com.querydsl.sql.Configuration;
import com.querydsl.sql.SQLListenerContext;
import com.querydsl.sql.SQLListeners;
import io.spine.server.storage.jdbc.ConnectionWrapper;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.query.given.Given.AStorageQuery;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.query.AbstractQuery.createFactory;
import static io.spine.server.storage.jdbc.query.given.Given.storageQueryBuilder;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * @author Dmytro Grankin
 */
@DisplayName("AbstractQuery should")
class AbstractQueryTest {

    private final DataSourceWrapper dataSource = whichIsStoredInMemory(newUuid());
    private final AStorageQuery query = storageQueryBuilder().setTableName(newUuid())
                                                             .setDataSource(dataSource)
                                                             .build();

    @Test
    @DisplayName("close connection")
    void closeConnection() throws SQLException {
        Configuration configuration = query.factory()
                                           .getConfiguration();
        SQLListenerContext context = mock(SQLListenerContext.class);
        Connection connection = mock(Connection.class);
        doReturn(connection).when(context)
                            .getConnection();
        configuration.getListeners()
                     .end(context);
        verify(connection).close();
    }

    /**
     * A commit is executed after a query execution, but a {@code ResultSet} should be used after
     * this, hence the result set should not be closed. This test verifies that the correct
     * holdability was set, assuming that the JDBC implementation does the rest correctly.
     */
    @Test
    @DisplayName("set `HOLD_CURSORS_OVER_COMMIT` for connection")
    void holdCursorsOverCommit() throws SQLException {
        DataSourceWrapper dataSourceSpy = spy(dataSource);
        ConnectionWrapper connectionWrapper = spy(dataSourceSpy.getConnection(true));
        Connection connection = spy(connectionWrapper.get());
        doReturn(connectionWrapper).when(dataSourceSpy)
                                   .getConnection(anyBoolean());
        doReturn(connection).when(connectionWrapper)
                            .get();
        AbstractSQLQueryFactory<?> factory = createFactory(dataSourceSpy);
        Connection connectionFromFactory = factory.getConnection();

        // The test can only verify that the holdability was set.
        // The result of the operation depends on a JDBC implementation.
        verify(connectionFromFactory).setHoldability(HOLD_CURSORS_OVER_COMMIT);
    }

    @Test
    @DisplayName("handle SQL exception on transaction rollback")
    void handleExceptionOnRollback() throws SQLException {
        Configuration configuration = query.factory()
                                           .getConfiguration();
        SQLListenerContext context = mock(SQLListenerContext.class);
        Connection connection = mock(Connection.class);
        doThrow(SQLException.class).when(connection)
                                   .rollback();
        doReturn(connection).when(context)
                            .getConnection();
        SQLListeners listeners = configuration.getListeners();
        assertThrows(DatabaseException.class, () -> listeners.exception(context));
    }

    @Test
    @DisplayName("handle SQL exception on transaction commit")
    void handleExceptionOnCommit() throws SQLException {
        Configuration configuration = query.factory()
                                           .getConfiguration();
        SQLListenerContext context = mock(SQLListenerContext.class);
        Connection connection = mock(Connection.class);
        doThrow(SQLException.class).when(connection)
                                   .commit();
        doReturn(connection).when(context)
                            .getConnection();
        SQLListeners listeners = configuration.getListeners();
        assertThrows(DatabaseException.class, () -> listeners.executed(context));
    }
}
