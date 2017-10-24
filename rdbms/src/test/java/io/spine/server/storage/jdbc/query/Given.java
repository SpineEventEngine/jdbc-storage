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

import com.google.protobuf.Message;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.GivenDataSource;
import io.spine.server.storage.jdbc.IdColumn;
import org.slf4j.Logger;

import java.sql.SQLException;

import static io.spine.server.storage.jdbc.GivenDataSource.whichThrowsExceptionOnExecuteStatement;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;

class Given {

    private static Logger loggerMock = null;
    @SuppressWarnings("unchecked") // OK for a mock.
    private static final IdColumn<String, String> ID_COLUMN_MOCK = mock(IdColumn.class);

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
                                   .setIdColumn(ID_COLUMN_MOCK);
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
        final EntityRecordWithColumns record =
                EntityRecordWithColumns.of(EntityRecord.getDefaultInstance());
        final WriteRecordQueryMock.Builder builder =
                WriteRecordQueryMock.newBuilder()
                                    .setDataSource(dataSourceMock)
                                    .setLogger(loggerMock)
                                    .setIdColumn(ID_COLUMN_MOCK)
                                    .setRecord(record);
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

    static SelectEventCountByIdQuery getSelectEventCountByIdQueryMock() throws SQLException {
        loggerMock = mock(Logger.class);
        final DataSourceWrapper dataSourceMock = whichThrowsExceptionOnExecuteStatement();
        final SelectEventCountByIdQuery.Builder<String> builder =
                SelectEventCountByIdQuery.<String>newBuilder(anyString())
                        .setDataSource(dataSourceMock)
                        .setLogger(loggerMock)
                        .setIdColumn(ID_COLUMN_MOCK);
        return builder.build();
    }

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

        @Override
        protected IdentifiedParameters getQueryParameters() {
            return IdentifiedParameters.empty();
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
