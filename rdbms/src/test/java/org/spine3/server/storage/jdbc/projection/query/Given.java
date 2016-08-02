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

package org.spine3.server.storage.jdbc.projection.query;

import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.spine3.server.storage.jdbc.GivenDataSource;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;

import java.sql.SQLException;

import static org.mockito.Mockito.*;

/**
 * @author Andrey Lavrov
 */
/* package */ class Given {

    /* package */ @SuppressWarnings("StaticNonFinalField")
    private static Logger loggerMock = null;

    private Given() {
    }

    /* package */ static InsertTimestampQuery getInsertTimestampQueryMock() throws SQLException {
        loggerMock = mock(Logger.class);
        final DataSourceWrapper dataSourceMock = GivenDataSource.whichThrowsExceptionOnSettingStatementParam();
        final InsertTimestampQuery.Builder builder = InsertTimestampQuery.newBuilder(anyString())
                .setDataSource(dataSourceMock)
                .setLogger(loggerMock)
                .setTimestamp(Timestamp.getDefaultInstance());
        return builder.build();
    }

    /* package */ static SelectTimestampQuery getSelectTimestampQueryMock() throws SQLException {
        loggerMock = mock(Logger.class);
        final DataSourceWrapper dataSourceMock = GivenDataSource.whichThrowsExceptionOnExecuteStatement();
        final SelectTimestampQuery.Builder builder = SelectTimestampQuery.newBuilder(anyString())
                .setDataSource(dataSourceMock)
                .setLogger(loggerMock);
        return builder.build();
    }

    /* package */ static UpdateTimestampQuery getUpdateTimestampQueryMock() throws SQLException {
        loggerMock = mock(Logger.class);
        final DataSourceWrapper dataSourceMock = GivenDataSource.whichThrowsExceptionOnSettingStatementParam();
        final UpdateTimestampQuery.Builder builder = UpdateTimestampQuery.newBuilder(anyString())
                .setDataSource(dataSourceMock)
                .setLogger(loggerMock)
                .setTimestamp(Timestamp.getDefaultInstance());
        return builder.build();
    }

    @SuppressWarnings("StaticVariableUsedBeforeInitialization")
    /* package */ static Logger getLoggerMock() {
        return loggerMock;
    }
}
