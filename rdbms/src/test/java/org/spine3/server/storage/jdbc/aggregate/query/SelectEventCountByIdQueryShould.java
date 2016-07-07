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

import org.junit.Test;
import org.slf4j.Logger;
import org.spine3.server.storage.jdbc.DataSourceMock;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import java.sql.SQLException;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

/**
 * @author Andrey Lavrov
 */
public class SelectEventCountByIdQueryShould {

    @Test
    @SuppressWarnings("DuplicateStringLiteralInspection")
    public void handle_sql_exception() throws SQLException {
        final Logger logger = mock(Logger.class);
        final DataSourceWrapper dataSourceMock = DataSourceMock.getMockDataSourceExceptionOnAnyExecute();
        final IdColumn idColumnMock = mock(IdColumn.class);

        final SelectEventCountByIdQuery query = SelectEventCountByIdQuery.newBuilder(anyString())
                .setDataSource(dataSourceMock)
                .setLogger(logger)
                .setIdColumn(idColumnMock)
                .build();
        try {
            query.execute();
            fail();
        } catch (DatabaseException expected) {
            verify(logger).error(anyString(), any(SQLException.class));
        }
    }
}
