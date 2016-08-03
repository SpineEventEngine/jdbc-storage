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

package org.spine3.server.storage.jdbc.query;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.spine3.server.storage.jdbc.GivenDataSource;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.mockito.Mockito.*;

/* package */ class Given {

    @SuppressWarnings("StaticNonFinalField")
    private static Logger loggerMock = null;
    private static final IdColumn<String> idColumnMock = mock(IdColumn.StringIdColumn.class);

    // TODO:2016-08-02:alexander.litus: these methods are used in one place only (each), so move them there.
    // Apply this for all such cases in all Given classes.

    // TODO:2016-08-02:alexander.litus: rename all such methods to `createTableQueryWhichThrows`.
    /* package */ static CreateTableQuery getCreateTableQueryMock() throws SQLException {
        loggerMock = mock(Logger.class);
        final DataSourceWrapper dataSourceMock = GivenDataSource.whichThrowsExceptionOnExecuteStatement();
        final CreateTableMock.Builder builder = CreateTableMock.newBuilder()
                .setDataSource(dataSourceMock)
                .setLogger(loggerMock);
        return builder.build();
    }

    /* package */ static SelectByIdQueryMock getSelectByIdQueryMock() throws SQLException {
        loggerMock = mock(Logger.class);
        final DataSourceWrapper dataSourceMock = GivenDataSource.whichThrowsExceptionOnExecuteStatement();
        final SelectByIdQueryMock.Builder builder = SelectByIdQueryMock.newBuilder()
                .setDataSource(dataSourceMock)
                .setLogger(loggerMock)
                .setIdColumn(idColumnMock);
        return builder.build();
    }

    /* package */ static SelectByIdQueryMock getSelectByIdQueryReturningEmptyResultSetMock() throws SQLException {
        loggerMock = mock(Logger.class);

        final DataSourceWrapper dataSourceMock = mock(DataSourceWrapper.class);
        final ConnectionWrapper connectionMock = mock(ConnectionWrapper.class);
        final PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
        final IdColumn<String> idColumnMock = mock(IdColumn.StringIdColumn.class);
        final ResultSet resultSetMock = mock(ResultSet.class);

        when(dataSourceMock.getConnection(anyBoolean())).thenReturn(connectionMock);
        when(connectionMock.prepareStatement(anyString())).thenReturn(preparedStatementMock);
        when(preparedStatementMock.executeQuery()).thenReturn(resultSetMock);
        when(resultSetMock.next()).thenReturn(true);
        when(resultSetMock.getBytes(anyString())).thenReturn(null);

        final SelectByIdQueryMock.Builder builder = SelectByIdQueryMock.newBuilder()
                .setDataSource(dataSourceMock)
                .setLogger(loggerMock)
                .setIdColumn(idColumnMock)
                .setMessageColumnName(anyString())
                .setMessageDescriptor(Any.getDescriptor());
        return builder.build();
    }

    /* package */ static WriteQueryMock getWriteQueryMock() throws SQLException {
        loggerMock = mock(Logger.class);
        final DataSourceWrapper dataSourceMock = GivenDataSource.whichThrowsExceptionOnExecuteStatement();
        final WriteQueryMock.Builder builder = WriteQueryMock.newBuilder()
                .setDataSource(dataSourceMock)
                .setLogger(loggerMock);
        return builder.build();
    }

    /* package */ static WriteRecordQueryMock getWriteRecordQueryMock() throws SQLException {
        loggerMock = mock(Logger.class);
        final DataSourceWrapper dataSourceMock = GivenDataSource.whichThrowsExceptionOnExecuteStatement();
        final WriteRecordQueryMock.Builder builder = WriteRecordQueryMock.newBuilder()
                .setDataSource(dataSourceMock)
                .setLogger(loggerMock)
                .setIdColumn(idColumnMock)
                .setRecord(recordMock);
        return builder.build();
    }

    private static final Any recordMock = Any.getDefaultInstance();

    private static class CreateTableMock extends CreateTableQuery<String> {

        protected CreateTableMock(Builder builder) {
            super(builder);
        }

        public static Builder newBuilder() {
            final Builder builder = new Builder();
            builder.setQuery("");
            return builder;
        }

        @SuppressWarnings("ClassNameSameAsAncestorName")
        public static class Builder extends CreateTableQuery.Builder<Builder, CreateTableMock, String> {

            @Override
            public CreateTableMock build() {
                return new CreateTableMock(this);
            }

            @Override
            protected Builder getThis() {
                return this;
            }
        }
    }

    private static class SelectByIdQueryMock extends SelectByIdQuery<String, Message> {

        protected SelectByIdQueryMock(Builder builder) {
            super(builder);
        }

        public static Builder newBuilder() {
            final Builder builder = new Builder();
            builder.setQuery("");
            return builder;
        }

        @SuppressWarnings("ClassNameSameAsAncestorName")
        public static class Builder extends SelectByIdQuery.Builder<Builder, SelectByIdQueryMock, String, Message> {

            @Override
            public SelectByIdQueryMock build() {
                return new SelectByIdQueryMock(this);
            }

            @Override
            protected Builder getThis() {
                return this;
            }
        }
    }

    private static class WriteRecordQueryMock extends WriteRecordQuery<String, Message> {

        protected WriteRecordQueryMock(Builder builder) {
            super(builder);
        }

        public static Builder newBuilder() {
            final Builder builder = new Builder();
            builder.setQuery("");
            return builder;
        }

        @SuppressWarnings("ClassNameSameAsAncestorName")
        public static class Builder extends WriteRecordQuery.Builder<Builder, WriteRecordQueryMock, String, Message> {

            @Override
            public WriteRecordQueryMock build() {
                return new WriteRecordQueryMock(this);
            }

            @Override
            protected Builder getThis() {
                return this;
            }
        }
    }

    private static class WriteQueryMock extends WriteQuery {

        protected WriteQueryMock(Builder builder) {
            super(builder);
        }

        public static Builder newBuilder() {
            final Builder builder = new Builder();
            builder.setQuery("");
            return builder;
        }

        @SuppressWarnings("ClassNameSameAsAncestorName")
        public static class Builder extends WriteQuery.Builder<Builder, WriteQueryMock> {

            @Override
            public WriteQueryMock build() {
                return new WriteQueryMock(this);
            }

            @Override
            protected Builder getThis() {
                return this;
            }
        }
    }

    private Given() {
    }

    @SuppressWarnings("StaticVariableUsedBeforeInitialization")
    /* package */ static Logger getLoggerMock() {
        return loggerMock;
    }
}
