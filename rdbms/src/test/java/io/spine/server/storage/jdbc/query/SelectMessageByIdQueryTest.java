/*
 * Copyright 2021, TeamDev. All rights reserved.
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

import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.querydsl.sql.AbstractSQLQuery;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.GivenDataSource.ThrowingHikariDataSource;
import io.spine.server.storage.jdbc.given.table.TimestampByString;
import io.spine.server.storage.jdbc.query.given.Given.ASelectMessageByIdQuery;
import io.spine.testing.logging.MuteLogging;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsThrowingByCommand;
import static io.spine.server.storage.jdbc.PredefinedMapping.H2_1_4;
import static io.spine.server.storage.jdbc.given.Column.stringIdColumn;
import static io.spine.server.storage.jdbc.message.MessageTable.bytesColumn;
import static io.spine.server.storage.jdbc.query.given.Given.selectMessageBuilder;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("SelectMessageByIdQuery should")
class SelectMessageByIdQueryTest {

    private final ASelectMessageByIdQuery.Builder<String> builder = selectMessageBuilder();

    @SuppressWarnings("CheckReturnValue") // Run method to close result set.
    @Test
    @DisplayName("close result set")
    void closeResultSet() throws SQLException {
        DataSourceWrapper dataSource = whichIsStoredInMemory(newUuid());
        TimestampByString table = table(dataSource);
        Timestamp timestamp = timestamp();
        table.write(timestamp);

        String id = table.idOf(timestamp);
        AbstractSQLQuery<Object, ?> underlyingQuery = table.composeSelectTimestampById(id)
                                                           .query();
        ASelectMessageByIdQuery query = query(dataSource, table, underlyingQuery);

        ResultSet results = underlyingQuery.getResults();
        query.execute();
        assertThat(results.isClosed())
                .isTrue();
    }

    @Test
    @MuteLogging
    @DisplayName("handle SQL exception")
    void handleSqlException() {
        ThrowingHikariDataSource underlyingDataSource = whichIsThrowingByCommand(newUuid());
        DataSourceWrapper dataSource = DataSourceWrapper.wrap(underlyingDataSource);
        TimestampByString table = table(dataSource);
        Timestamp timestamp = timestamp();
        table.write(timestamp);

        String id = table.idOf(timestamp);
        AbstractSQLQuery<Object, ?> underlyingQuery = table.composeSelectTimestampById(id)
                                                           .query();
        ASelectMessageByIdQuery query = query(dataSource, table, underlyingQuery);

        underlyingDataSource.setThrowOnGetConnection(true);
        assertThrows(DatabaseException.class, query::execute);
    }

    @Test
    @DisplayName("return `null` on deserialization if value is not present")
    void returnNullForValueNotPresent() {
        DataSourceWrapper dataSource = whichIsStoredInMemory(newUuid());
        TimestampByString table = table(dataSource);
        Timestamp timestamp = Timestamp.getDefaultInstance();

        String id = table.idOf(timestamp);
        AbstractSQLQuery<Object, ?> underlyingQuery = table.composeSelectTimestampById(id)
                                                           .query();
        ASelectMessageByIdQuery query = query(dataSource, table, underlyingQuery);

        Message message = query.execute();
        assertThat(message)
                .isNull();
    }

    private ASelectMessageByIdQuery query(DataSourceWrapper dataSource,
                                          TimestampByString table,
                                          AbstractSQLQuery<Object, ?> underlyingQuery) {
        ASelectMessageByIdQuery<String> query =
                builder.setTableName(table.name())
                       .setQuery(underlyingQuery)
                       .setDataSource(dataSource)
                       .setId(newUuid())
                       .setIdColumn(stringIdColumn())
                       .setMessageColumnName(bytesColumn().name())
                       .setMessageDescriptor(Timestamp.getDescriptor())
                       .build();
        return query;
    }

    private static TimestampByString table(DataSourceWrapper dataSource) {
        TimestampByString table = new TimestampByString(dataSource, H2_1_4);
        table.create();
        return table;
    }

    private static Timestamp timestamp() {
        Timestamp timestamp = Timestamp
                .newBuilder()
                .setSeconds(42)
                .setNanos(15)
                .build();
        return timestamp;
    }
}
