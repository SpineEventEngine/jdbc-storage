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

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import com.querydsl.sql.AbstractSQLQuery;
import com.querydsl.sql.SQLQuery;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.DatabaseException;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static io.spine.Identifier.newUuid;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.query.IdColumn.typeString;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Dmytro Grankin
 */
public class SelectMessageByIdQueryShould {

    @Test
    public void close_result_set() throws SQLException {
        final SQLQuery underlyingQuery = mock(SQLQuery.class);
        final ResultSet resultSet = mock(ResultSet.class);

        doReturn(resultSet).when(underlyingQuery)
                           .getResults();
        final DataSourceWrapper dataSource = whichIsStoredInMemory(newUuid());
        final ASelectMessageByIdQuery query = newBuilder().setTableName(newUuid())
                                                          .setQuery(underlyingQuery)
                                                          .setDataSource(dataSource)
                                                          .setId(newUuid())
                                                          .setIdColumn(typeString(newUuid()))
                                                          .build();
        query.execute();
        verify(resultSet).close();
    }

    @Test(expected = DatabaseException.class)
    public void handle_sql_exception() throws SQLException {
        final SQLQuery underlyingQuery = mock(SQLQuery.class);
        final ResultSet resultSet = mock(ResultSet.class);

        doReturn(resultSet).when(underlyingQuery)
                           .getResults();
        doThrow(SQLException.class).when(resultSet)
                                   .next();
        final DataSourceWrapper dataSource = whichIsStoredInMemory(newUuid());
        final ASelectMessageByIdQuery query = newBuilder().setTableName(newUuid())
                                                          .setQuery(underlyingQuery)
                                                          .setDataSource(dataSource)
                                                          .setId(newUuid())
                                                          .setIdColumn(typeString(newUuid()))
                                                          .build();
        query.execute();
    }

    @Test
    public void return_null_on_deserialization_if_column_is_null() throws SQLException {
        final ResultSet resultSet = mock(ResultSet.class);
        final DataSourceWrapper dataSource = whichIsStoredInMemory(newUuid());
        final Descriptors.Descriptor messageDescriptor = StringValue.getDescriptor();
        final ASelectMessageByIdQuery query = newBuilder().setTableName(newUuid())
                                                          .setMessageColumnName(newUuid())
                                                          .setMessageDescriptor(messageDescriptor)
                                                          .setDataSource(dataSource)
                                                          .setId(newUuid())
                                                          .setIdColumn(typeString(newUuid()))
                                                          .build();
        final Message deserialized = query.readMessage(resultSet);
        assertNull(deserialized);
    }

    private static class ASelectMessageByIdQuery extends SelectMessageByIdQuery<String, Message> {


        private final AbstractSQLQuery<?, ?> query;
        private ASelectMessageByIdQuery(Builder builder) {
            super(builder);
            this.query = builder.query;
        }

        @Override
        protected AbstractSQLQuery<?, ?> getQuery() {
            return query;
        }

        private static class Builder extends SelectMessageByIdQuery.Builder<Builder,
                                                                            ASelectMessageByIdQuery,
                                                                            String,
                                                                            Message> {

            private AbstractSQLQuery<?, ?> query;

            private Builder setQuery(AbstractSQLQuery<?, ?> query) {
                this.query = query;
                return this;
            }

            @Override
            protected ASelectMessageByIdQuery doBuild() {
                return new ASelectMessageByIdQuery(this);
            }

            @Override
            protected Builder getThis() {
                return this;
            }
        }
    }

    static ASelectMessageByIdQuery.Builder newBuilder() {
        return new ASelectMessageByIdQuery.Builder();
    }
}
