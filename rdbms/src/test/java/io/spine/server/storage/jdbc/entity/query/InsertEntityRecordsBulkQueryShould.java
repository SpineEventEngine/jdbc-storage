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

package io.spine.server.storage.jdbc.entity.query;

import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.GivenDataSource;
import org.junit.Test;
import org.slf4j.Logger;
import io.spine.server.entity.EntityRecord;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.util.ConnectionWrapper;
import io.spine.server.storage.jdbc.util.DataSourceWrapper;
import io.spine.server.storage.jdbc.util.IdColumn;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Dmytro Dashenkov
 */
public class InsertEntityRecordsBulkQueryShould {

    @Test
    public void throw_DatabaseException_on_SqlException_and_log() throws SQLException {
        final EntityRecordWithColumns entityRecord = EntityRecordWithColumns.of(EntityRecord.getDefaultInstance());
        final Map<String, EntityRecordWithColumns> recordMap = new HashMap<>(1);
        final String id = "fake-id";
        recordMap.put(id, entityRecord);

        final DataSourceWrapper dataSource = GivenDataSource.whichIsStoredInMemory(
                "InsertEntityRecordsBulkQueryShould");
        final Logger logger = mock(Logger.class);

        final InsertEntityRecordsBulkQuery<String> query =
                InsertEntityRecordsBulkQuery.<String>newBuilder()
                        .setLogger(logger)
                        .setDataSource(dataSource)
                        .setRecords(recordMap)
                        .setidColumn(IdColumn.typeString("id"))
                        .setTableName("random-table")
                        .build();

        final ConnectionWrapper wrapper = mock(ConnectionWrapper.class);
        final PreparedStatement statement = mock(PreparedStatement.class);
        when(wrapper.prepareStatement(anyString())).thenReturn(statement);
        @SuppressWarnings("ThrowableNotThrown")
        final SQLException exception = new SQLException("Nothing to worry about");
        doThrow(exception).when(statement).setBytes(anyInt(), any(byte[].class));

        try {
            query.prepareStatement(wrapper);
        } catch (DatabaseException e) {
            verify(logger).error(contains(id), eq(exception));
        }
    }
}
