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

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.ConnectionWrapper;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.GivenDataSource;
import io.spine.server.storage.jdbc.IdColumn;
import org.slf4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static io.spine.server.storage.jdbc.GivenDataSource.whichThrowsExceptionOnExecuteStatement;
import static io.spine.server.storage.jdbc.GivenDataSource.whichThrowsExceptionOnSettingStatementParam;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class Given {

    private static Logger loggerMock = null;
    @SuppressWarnings("unchecked") // OK for a mock.
    private static final IdColumn<String> idColumnMock = mock(IdColumn.class);

    private Given() {
        // Prevent utility class instantiation.
    }

    static SelectByIdQueryMock getSelectByIdQueryMock() throws SQLException {
        loggerMock = mock(Logger.class);
        final DataSourceWrapper dataSourceMock =
                GivenDataSource.whichThrowsExceptionOnExecuteStatement();
        final SelectByIdQueryMock.Builder builder =
                SelectByIdQueryMock.newBuilder()
                                   .setDataSource(dataSourceMock)
                                   .setLogger(loggerMock)
                                   .setIdColumn(idColumnMock);
        return builder.build();
    }

    static SelectByIdQueryMock getSelectByIdQueryReturningEmptyResultSetMock() throws SQLException {
        loggerMock = mock(Logger.class);

        final DataSourceWrapper dataSourceMock = mock(DataSourceWrapper.class);
        final ConnectionWrapper connectionMock = mock(ConnectionWrapper.class);
        final PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
        @SuppressWarnings("unchecked") // OK for a mock.
        final IdColumn<String> idColumnMock = mock(IdColumn.class);
        final ResultSet resultSetMock = mock(ResultSet.class);

        when(dataSourceMock.getConnection(anyBoolean())).thenReturn(connectionMock);
        when(connectionMock.prepareStatement(anyString())).thenReturn(preparedStatementMock);
        when(preparedStatementMock.executeQuery()).thenReturn(resultSetMock);
        when(resultSetMock.next()).thenReturn(true);
        when(resultSetMock.getBytes(anyString())).thenReturn(null);

        final SelectByIdQueryMock.Builder builder =
                SelectByIdQueryMock.newBuilder()
                                   .setDataSource(dataSourceMock)
                                   .setLogger(loggerMock)
                                   .setIdColumn(idColumnMock)
                                   .setMessageColumnName(anyString())
                                   .setMessageDescriptor(Any.getDescriptor());
        return builder.build();
    }

    static WriteQueryMock getWriteQueryMock() throws SQLException {
        loggerMock = mock(Logger.class);
        final DataSourceWrapper dataSourceMock =
                GivenDataSource.whichThrowsExceptionOnExecuteStatement();
        final WriteQueryMock.Builder builder = WriteQueryMock.newBuilder()
                                                             .setDataSource(dataSourceMock)
                                                             .setLogger(loggerMock);
        return builder.build();
    }

    static WriteRecordQueryMock getWriteRecordQueryMock() throws SQLException {
        loggerMock = mock(Logger.class);
        final DataSourceWrapper dataSourceMock =
                GivenDataSource.whichThrowsExceptionOnExecuteStatement();
        final WriteRecordQueryMock.Builder builder =
                WriteRecordQueryMock.newBuilder()
                                    .setDataSource(dataSourceMock)
                                    .setLogger(loggerMock)
                                    .setIdColumn(idColumnMock)
                                    .setRecord(recordMock);
        return builder.build();
    }

    static InsertTimestampQuery getInsertTimestampQueryMock() throws SQLException {
        loggerMock = mock(Logger.class);
        final DataSourceWrapper dataSourceMock = whichThrowsExceptionOnSettingStatementParam();
        final InsertTimestampQuery.Builder builder =
                InsertTimestampQuery.newBuilder(anyString())
                                    .setDataSource(dataSourceMock)
                                    .setLogger(loggerMock)
                                    .setTimestamp(Timestamp.getDefaultInstance());
        return builder.build();
    }

    static SelectTimestampQuery getSelectTimestampQueryMock() throws SQLException {
        loggerMock = mock(Logger.class);
        final DataSourceWrapper dataSourceMock = whichThrowsExceptionOnExecuteStatement();
        final SelectTimestampQuery.Builder builder =
                SelectTimestampQuery.newBuilder(anyString())
                                    .setDataSource(dataSourceMock)
                                    .setIdColumn(IdColumn.typeString("id"))
                                    .setLogger(loggerMock);
        return builder.build();
    }

    static UpdateTimestampQuery getUpdateTimestampQueryMock() throws SQLException {
        loggerMock = mock(Logger.class);
        final DataSourceWrapper dataSourceMock = whichThrowsExceptionOnSettingStatementParam();
        final UpdateTimestampQuery.Builder builder =
                UpdateTimestampQuery.newBuilder(anyString())
                                    .setDataSource(dataSourceMock)
                                    .setLogger(loggerMock)
                                    .setTimestamp(Timestamp.getDefaultInstance());
        return builder.build();
    }

    private static final IdColumn<String> ID_COLUMN_QUERY_SETTER_MOCK = mock(IdColumn.class);

    static InsertAggregateRecordQuery getInsertAggregateRecordQueryMock() throws SQLException {
        loggerMock = mock(Logger.class);
        final DataSourceWrapper dataSourceMock = whichThrowsExceptionOnSettingStatementParam();
        final InsertAggregateRecordQuery.Builder<String> builder =
                InsertAggregateRecordQuery.<String>newBuilder(anyString())
                        .setDataSource(dataSourceMock)
                        .setLogger(loggerMock)
                        .setIdColumn(ID_COLUMN_QUERY_SETTER_MOCK)
                        .setRecord(AggregateEventRecord.getDefaultInstance()
                                                       .getDefaultInstanceForType());
        return builder.build();
    }

    static InsertEventCountQuery getInsertEventCountQueryMock() throws SQLException {
        loggerMock = mock(Logger.class);
        final DataSourceWrapper dataSourceMock = whichThrowsExceptionOnSettingStatementParam();
        final InsertEventCountQuery.Builder<String> builder =
                InsertEventCountQuery.<String>newBuilder(anyString())
                        .setDataSource(dataSourceMock)
                        .setLogger(loggerMock)
                        .setIdColumn(ID_COLUMN_QUERY_SETTER_MOCK);
        return builder.build();
    }

    static SelectEventCountByIdQuery getSelectEventCountByIdQueryMock() throws SQLException {
        loggerMock = mock(Logger.class);
        final DataSourceWrapper dataSourceMock = whichThrowsExceptionOnExecuteStatement();
        final SelectEventCountByIdQuery.Builder<String> builder =
                SelectEventCountByIdQuery.<String>newBuilder(anyString())
                        .setDataSource(dataSourceMock)
                        .setLogger(loggerMock)
                        .setIdColumn(ID_COLUMN_QUERY_SETTER_MOCK);
        return builder.build();
    }

    static UpdateEventCountQuery getUpdateEventCountQueryMock() throws SQLException {
        loggerMock = mock(Logger.class);
        final DataSourceWrapper dataSourceMock = whichThrowsExceptionOnSettingStatementParam();
        final UpdateEventCountQuery.Builder<String> builder =
                UpdateEventCountQuery.<String>newBuilder(anyString())
                        .setDataSource(dataSourceMock)
                        .setLogger(loggerMock)
                        .setIdColumn(ID_COLUMN_QUERY_SETTER_MOCK);
        return builder.build();
    }

    private static final EntityRecordWithColumns recordMock =
            EntityRecordWithColumns.of(EntityRecord.getDefaultInstance());

    private static class SelectByIdQueryMock extends SelectMessageByIdQuery<String, Message> {

        protected SelectByIdQueryMock(Builder builder) {
            super(builder);
        }

        public static Builder newBuilder() {
            final Builder builder = new Builder();
            builder.setQuery("");
            return builder;
        }

        @SuppressWarnings("ClassNameSameAsAncestorName")
        public static class Builder extends SelectMessageByIdQuery.Builder<Builder,
                                                                           SelectByIdQueryMock,
                                                                           String,
                                                                           Message> {

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
        public static class Builder extends WriteRecordQuery.Builder<Builder,
                                                                     WriteRecordQueryMock,
                                                                     String,
                                                                     Message> {

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

    static Logger getLoggerMock() {
        return loggerMock;
    }
}
