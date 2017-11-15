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

import com.querydsl.sql.AbstractSQLQueryFactory;
import com.querydsl.sql.Configuration;
import com.querydsl.sql.SQLListenerContext;
import com.querydsl.sql.SQLListeners;
import io.spine.server.storage.jdbc.ConnectionWrapper;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.query.given.Given.AStorageQuery;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static io.spine.Identifier.newUuid;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.query.AbstractQuery.createFactory;
import static io.spine.server.storage.jdbc.query.given.Given.storageQueryBuilder;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * @author Dmytro Grankin
 */
public class AbstractQueryShould {

    private final DataSourceWrapper dataSource = whichIsStoredInMemory(newUuid());
    private final AStorageQuery query = storageQueryBuilder().setTableName(newUuid())
                                                             .setDataSource(dataSource)
                                                             .build();

    @Test
    public void close_connection() throws SQLException {
        final Configuration configuration = query.factory()
                                                 .getConfiguration();
        final SQLListenerContext context = mock(SQLListenerContext.class);
        final Connection connection = mock(Connection.class);
        doReturn(connection).when(context)
                            .getConnection();
        configuration.getListeners()
                     .end(context);
        verify(connection).close();
    }

    @Test
    public void set_hold_cursors_over_commit_for_connection() throws SQLException {
        final DataSourceWrapper dataSourceSpy = spy(dataSource);
        final ConnectionWrapper connectionWrapper = spy(dataSourceSpy.getConnection(true));
        final Connection connection = spy(connectionWrapper.get());
        doReturn(connectionWrapper).when(dataSourceSpy)
                                   .getConnection(anyBoolean());
        doReturn(connection).when(connectionWrapper)
                            .get();
        final AbstractSQLQueryFactory<?> factory = createFactory(dataSourceSpy);
        final Connection connectionFromFactory = factory.getConnection();

        // The test can only verify that the holdability was set.
        // The result of the operation depends on a JDBC implementation.
        verify(connectionFromFactory).setHoldability(HOLD_CURSORS_OVER_COMMIT);
    }

    @Test(expected = DatabaseException.class)
    public void handle_sql_exception_on_transaction_rollback() throws SQLException {
        final Configuration configuration = query.factory()
                                                 .getConfiguration();
        final SQLListenerContext context = mock(SQLListenerContext.class);
        final Connection connection = mock(Connection.class);
        doThrow(SQLException.class).when(connection)
                                   .rollback();
        doReturn(connection).when(context)
                            .getConnection();
        final SQLListeners listeners = configuration.getListeners();
        listeners.exception(context);
    }

    @Test(expected = DatabaseException.class)
    public void handle_sql_exception_on_transaction_commit() throws SQLException {
        final Configuration configuration = query.factory()
                                                 .getConfiguration();
        final SQLListenerContext context = mock(SQLListenerContext.class);
        final Connection connection = mock(Connection.class);
        doThrow(SQLException.class).when(connection)
                                   .commit();
        doReturn(connection).when(context)
                            .getConnection();
        final SQLListeners listeners = configuration.getListeners();
        listeners.executed(context);
    }
}
