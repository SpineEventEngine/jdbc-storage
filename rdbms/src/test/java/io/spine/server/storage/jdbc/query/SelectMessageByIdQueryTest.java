/*
 * Copyright 2023, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.protobuf.Timestamp;
import com.querydsl.sql.AbstractSQLQuery;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.JdbcStorageFactory;
import io.spine.server.storage.jdbc.given.table.TimestampByString;
import io.spine.server.storage.jdbc.query.given.Given.ASelectMessageByIdQuery;
import io.spine.testing.logging.mute.MuteLogging;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsThrowingByCommand;
import static io.spine.server.storage.jdbc.PredefinedMapping.H2_2_1;
import static io.spine.server.storage.jdbc.query.given.Given.selectMsgBuilder;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("`SelectMessageByIdQuery` should")
class SelectMessageByIdQueryTest {

    private final ASelectMessageByIdQuery.Builder<String, Timestamp> builder = selectMsgBuilder();

    @SuppressWarnings("CheckReturnValue") // Run method to close result set.
    @Test
    @DisplayName("close result set")
    void closeResultSet() throws SQLException {
        var dataSource = whichIsStoredInMemory(newUuid());
        var table = table(dataSource);
        var timestamp = timestamp();
        table.write(timestamp);

        var id = table.idOf(timestamp);
        var underlyingQuery = table.composeSelectTimestampById(id)
                                   .query();
        var query = query(dataSource, table, underlyingQuery);

        var results = underlyingQuery.getResults();
        var ignored = query.execute();
        assertThat(results.isClosed())
                .isTrue();
    }

    @Test
    @MuteLogging
    @DisplayName("handle SQL exception")
    void handleSqlException() {
        var underlyingDataSource = whichIsThrowingByCommand(newUuid());
        var dataSource = DataSourceWrapper.wrap(underlyingDataSource);
        var table = table(dataSource);
        var timestamp = timestamp();
        table.write(timestamp);

        var id = table.idOf(timestamp);
        var underlyingQuery = table.composeSelectTimestampById(id)
                                   .query();
        var query = query(dataSource, table, underlyingQuery);

        underlyingDataSource.setThrowOnGetConnection(true);
        assertThrows(DatabaseException.class, query::execute);
    }

    @Test
    @DisplayName("return `null` on deserialization if value is not present")
    void returnNullForValueNotPresent() {
        var dataSource = whichIsStoredInMemory(newUuid());
        var table = table(dataSource);
        var timestamp = Timestamp.getDefaultInstance();

        var id = table.idOf(timestamp);
        var underlyingQuery = table.composeSelectTimestampById(id)
                                   .query();
        var query = query(dataSource, table, underlyingQuery);

        var message = query.execute();
        assertThat(message)
                .isNull();
    }

    private ASelectMessageByIdQuery<String, Timestamp>
    query(DataSourceWrapper dataSource,
          TimestampByString table,
          AbstractSQLQuery<Object, ?> underlyingQuery) {
        var query = builder.setTableSpec(table.spec())
                           .setQuery(underlyingQuery)
                           .setDataSource(dataSource)
                           .setId(newUuid())
                           .build();
        return query;
    }

    private static TimestampByString table(DataSourceWrapper dataSource) {
        var factory = JdbcStorageFactory.newBuilder()
                .setDataSource(dataSource)
                .setTypeMapping(H2_2_1)
                .build();
        var table = new TimestampByString(factory);
        table.create();
        return table;
    }

    private static Timestamp timestamp() {
        var timestamp = Timestamp.newBuilder()
                .setSeconds(42)
                .setNanos(15)
                .build();
        return timestamp;
    }
}
