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

package io.spine.server.storage.jdbc.aggregate.query;

import io.spine.server.storage.jdbc.query.InsertAggregateRecordQuery;
import io.spine.server.storage.jdbc.query.InsertEventCountQuery;
import io.spine.server.storage.jdbc.query.SelectEventCountByIdQuery;
import io.spine.server.storage.jdbc.query.UpdateEventCountQuery;
import org.slf4j.Logger;
import io.spine.server.aggregate.AggregateEventRecord;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.IdColumn;

import java.sql.SQLException;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static io.spine.server.storage.jdbc.GivenDataSource.whichThrowsExceptionOnExecuteStatement;
import static io.spine.server.storage.jdbc.GivenDataSource.whichThrowsExceptionOnSettingStatementParam;

/**
 * @author Andrey Lavrov
 */
@SuppressWarnings("UtilityClass")
class Given {

    private static Logger loggerMock = null;
    @SuppressWarnings("unchecked") // OK for a mock.
    private static final IdColumn<String> ID_COLUMN_QUERY_SETTER_MOCK = mock(IdColumn.class);

    private Given() {
    }

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

    static Logger getLoggerMock() {
        return loggerMock;
    }
}
