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

package org.spine3.server.storage.jdbc.event.query;

import org.slf4j.Logger;
import org.spine3.base.Event;
import org.spine3.server.storage.jdbc.GivenDataSource;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import java.sql.SQLException;

import static org.mockito.Mockito.mock;

/**
 * @author Andrey Lavrov
 */
@SuppressWarnings("UtilityClass")
class Given {

    @SuppressWarnings("StaticNonFinalField")
    private static Logger loggerMock = null;
    private static final IdColumn<String> idColumnMock = mock(IdColumn.StringIdColumn.class);

    private Given() {
    }

    static InsertEventRecordQuery getInsertEventRecordQueryMock() throws SQLException {
        loggerMock = mock(Logger.class);
        final DataSourceWrapper dataSourceMock = GivenDataSource.whichThrowsExceptionOnSettingStatementParam();
        final InsertEventRecordQuery.Builder builder = InsertEventRecordQuery.newBuilder()
                                                                             .setDataSource(
                                                                                     dataSourceMock)
                                                                             .setLogger(loggerMock)
                                                                             .setIdColumn(
                                                                                     idColumnMock)
                                                                             .setRecord(
                                                                                     Event.getDefaultInstance());
        return builder.build();
    }

    static UpdateEventRecordQuery getUpdateEventRecordQueryMock() throws SQLException {
        loggerMock = mock(Logger.class);
        final DataSourceWrapper dataSourceMock = GivenDataSource.whichThrowsExceptionOnSettingStatementParam();
        final UpdateEventRecordQuery.Builder builder = UpdateEventRecordQuery.newBuilder()
                                                                             .setDataSource(
                                                                                     dataSourceMock)
                                                                             .setLogger(loggerMock)
                                                                             .setIdColumn(
                                                                                     idColumnMock)
                                                                             .setRecord(
                                                                                     Event.getDefaultInstance());
        return builder.build();
    }

    @SuppressWarnings("StaticVariableUsedBeforeInitialization")
    static Logger getLoggerMock() {
        return loggerMock;
    }
}
