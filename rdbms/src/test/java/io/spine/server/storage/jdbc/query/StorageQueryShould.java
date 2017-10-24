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
import org.junit.Test;
import org.slf4j.Logger;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static io.spine.Identifier.newUuid;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * @author Dmytro Grankin
 */
public class StorageQueryShould {

    @Test(expected = DatabaseException.class)
    public void handle_sql_exception() throws SQLException {
        final Logger logger = mock(Logger.class);
        final ConnectionWrapper connection = mock(ConnectionWrapper.class);
        final PreparedStatement statement = mock(PreparedStatement.class);

        final int parameterId = 1;
        final Object parameterValue = new Object();
        final StorageQuery query = new Builder().addParameter(parameterId, parameterValue)
                                                .setQuery(newUuid())
                                                .build();
        final StorageQuery querySpy = spy(query);
        doReturn(statement).when(connection)
                           .prepareStatement(anyString());
        doThrow(SQLException.class).when(statement)
                                   .setObject(parameterId, parameterValue);
        doReturn(logger).when(querySpy)
                        .getLogger();
        querySpy.prepareStatement(connection);
    }

    private static class AStorageQuery extends StorageQuery {

        private final Parameters parameters;

        private AStorageQuery(StorageQueryShould.Builder builder) {
            super(builder);
            this.parameters = builder.parameters.build();
        }

        @Override
        protected Parameters getQueryParameters() {
            return parameters;
        }
    }

    private static class Builder extends StorageQuery.Builder<Builder, AStorageQuery> {

        private final Parameters.Builder parameters = Parameters.newBuilder();

        private Builder addParameter(Integer id, Object value) {
            parameters.addParameter(id, value);
            return getThis();
        }

        @Override
        public AStorageQuery build() {
            return new AStorageQuery(this);
        }

        @Override
        protected Builder getThis() {
            return this;
        }
    }
}
