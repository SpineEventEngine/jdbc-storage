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

import com.querydsl.sql.Configuration;
import com.querydsl.sql.SQLListenerContext;
import com.querydsl.sql.SQLListeners;
import io.spine.server.storage.jdbc.DatabaseException;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static io.spine.Identifier.newUuid;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Dmytro Grankin
 */
public class AbstractQueryShould {

    private final AStorageQuery query = new Builder().setTableName(newUuid())
                                                     .setDataSource(whichIsStoredInMemory(newUuid()))
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

    private static class AStorageQuery extends AbstractQuery {

        private AStorageQuery(AbstractQueryShould.Builder builder) {
            super(builder);
        }
    }

    private static class Builder extends AbstractQuery.Builder<Builder, AStorageQuery> {

        @Override
        AStorageQuery build() {
            return new AStorageQuery(this);
        }

        @Override
        Builder getThis() {
            return this;
        }
    }
}
