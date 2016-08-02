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

package org.spine3.server.storage.jdbc.aggregate.query;

import org.slf4j.Logger;
import org.spine3.server.storage.AggregateStorageRecord;
import org.spine3.server.storage.jdbc.GivenDataSource;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import java.sql.SQLException;

import static org.mockito.Mockito.*;

/**
 * @author Andrey Lavrov
 */
@SuppressWarnings("UtilityClass")
/* package */ class Given {

    @SuppressWarnings("StaticNonFinalField")
    private static Logger loggerMock = null;
    private static final IdColumn<String> idColumnMock = mock(IdColumn.StringIdColumn.class);

    private Given() {
    }

    /* package */ static InsertAggregateRecordQuery getInsertAggregateRecordQueryMock() throws SQLException {
        loggerMock = mock(Logger.class);
        final DataSourceWrapper dataSourceMock = GivenDataSource.whichThrowsExceptionOnSettingStatementParam();
        final InsertAggregateRecordQuery.Builder<String> builder = InsertAggregateRecordQuery.<String>newBuilder(anyString())
                .setDataSource(dataSourceMock)
                .setLogger(loggerMock)
                .setIdColumn(idColumnMock)
                .setRecord(AggregateStorageRecord.getDefaultInstance().getDefaultInstanceForType());
        return builder.build();
    }

    /* package */ static InsertEventCountQuery getInsertEventCountQueryMock() throws SQLException {
        loggerMock = mock(Logger.class);
        final DataSourceWrapper dataSourceMock = GivenDataSource.whichThrowsExceptionOnSettingStatementParam();
        final InsertEventCountQuery.Builder<String> builder = InsertEventCountQuery.<String>newBuilder(anyString())
                .setDataSource(dataSourceMock)
                .setLogger(loggerMock)
                .setIdColumn(idColumnMock);
        return builder.build();
    }

    /* package */ static SelectEventCountByIdQuery getSelectEventCountByIdQueryMock() throws SQLException {
        loggerMock = mock(Logger.class);
        final DataSourceWrapper dataSourceMock = GivenDataSource.whichThrowsExceptionOnExecuteStatement();
        final SelectEventCountByIdQuery.Builder<String> builder = SelectEventCountByIdQuery.<String>newBuilder(anyString())
                .setDataSource(dataSourceMock)
                .setLogger(loggerMock)
                .setIdColumn(idColumnMock);
        return builder.build();
    }

    /* package */ static UpdateEventCountQuery getUpdateEventCountQueryMock() throws SQLException {
        loggerMock = mock(Logger.class);
        final DataSourceWrapper dataSourceMock = GivenDataSource.whichThrowsExceptionOnSettingStatementParam();
        final UpdateEventCountQuery.Builder<String> builder = UpdateEventCountQuery.<String>newBuilder(anyString())
                .setDataSource(dataSourceMock)
                .setLogger(loggerMock)
                .setIdColumn(idColumnMock);
        return builder.build();
    }

    @SuppressWarnings("StaticVariableUsedBeforeInitialization")
    /* package */ static Logger getLoggerMock() {
        return loggerMock;
    }
}
