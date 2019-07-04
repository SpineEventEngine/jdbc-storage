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

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import com.querydsl.sql.SQLQuery;
import io.spine.server.storage.jdbc.DataSourceSupplier;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.query.given.Given.ASelectMessageByIdQuery;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.query.IdColumn.typeString;
import static io.spine.server.storage.jdbc.query.given.Given.selectMessageBuilder;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@DisplayName("SelectMessageByIdQuery should")
class SelectMessageByIdQueryTest {

    private final ASelectMessageByIdQuery.Builder builder = selectMessageBuilder();

    @SuppressWarnings("CheckReturnValue") // Run method to close result set.
    @Test
    @DisplayName("close result set")
    void closeResultSet() throws SQLException {
        SQLQuery underlyingQuery = mock(SQLQuery.class);
        ResultSet resultSet = mock(ResultSet.class);

        doReturn(resultSet).when(underlyingQuery)
                           .getResults();
        DataSourceSupplier dataSource = whichIsStoredInMemory(newUuid());
        ASelectMessageByIdQuery query = builder.setTableName(newUuid())
                                               .setQuery(underlyingQuery)
                                               .setDataSource(dataSource)
                                               .setId(newUuid())
                                               .setIdColumn(typeString(newUuid()))
                                               .build();
        query.execute();
        verify(resultSet).close();
    }

    @Test
    @DisplayName("handle SQL exception")
    void handleSqlException() throws SQLException {
        SQLQuery underlyingQuery = mock(SQLQuery.class);
        ResultSet resultSet = mock(ResultSet.class);

        doReturn(resultSet).when(underlyingQuery)
                           .getResults();
        doThrow(SQLException.class).when(resultSet)
                                   .next();
        DataSourceSupplier dataSource = whichIsStoredInMemory(newUuid());
        ASelectMessageByIdQuery query = builder.setTableName(newUuid())
                                               .setQuery(underlyingQuery)
                                               .setDataSource(dataSource)
                                               .setId(newUuid())
                                               .setIdColumn(typeString(newUuid()))
                                               .build();

        assertThrows(DatabaseException.class, query::execute);
    }

    @Test
    @DisplayName("return null on deserialization if column is null")
    void returnNullForNullColumn() throws SQLException {
        ResultSet resultSet = mock(ResultSet.class);
        DataSourceSupplier dataSource = whichIsStoredInMemory(newUuid());
        Descriptors.Descriptor messageDescriptor = StringValue.getDescriptor();
        ASelectMessageByIdQuery query = builder.setTableName(newUuid())
                                               .setMessageColumnName(newUuid())
                                               .setMessageDescriptor(messageDescriptor)
                                               .setDataSource(dataSource)
                                               .setId(newUuid())
                                               .setIdColumn(typeString(newUuid()))
                                               .build();
        Message deserialized = query.readMessage(resultSet);
        assertNull(deserialized);
    }
}
