/*
 * Copyright 2020, TeamDev. All rights reserved.
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
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.given.table.TimestampByLong;
import io.spine.server.storage.jdbc.given.table.TimestampByMessage;
import io.spine.server.storage.jdbc.given.table.TimestampByString;
import io.spine.server.storage.jdbc.message.MessageTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.PredefinedMapping.H2_1_4;
import static io.spine.server.storage.jdbc.query.ColumnReaderFactory.idReader;
import static io.spine.server.storage.jdbc.query.ColumnReaderFactory.messageReader;

@DisplayName("ColumnReader should")
class ColumnReaderTest {

    private DataSourceWrapper dataSource;

    @BeforeEach
    void setUpDataSource() {
        dataSource = whichIsStoredInMemory(newUuid());
    }

    @Test
    @DisplayName("read the value of an ID column of Number type")
    void readNumberId() throws SQLException {
        TimestampByLong table = new TimestampByLong(dataSource, H2_1_4);
        table.create();

        Timestamp timestamp = timestamp();
        table.write(timestamp);
        String columnName = table.idColumn()
                                 .columnName();
        ColumnReader<Long> reader = idReader(columnName, Long.class);

        Long id = table.idOf(timestamp);
        ResultSet resultSet = table.resultSetWithId(id);
        assertThat(resultSet.next())
                .isTrue();

        Long acquiredId = reader.readValue(resultSet);
        assertThat(acquiredId)
                .isEqualTo(id);
    }

    @Test
    @DisplayName("read the value of an ID column of String type")
    void readStringId() throws SQLException {
        TimestampByString table = new TimestampByString(dataSource, H2_1_4);
        table.create();

        Timestamp timestamp = timestamp();
        table.write(timestamp);
        String columnName = table.idColumn()
                                 .columnName();
        ColumnReader<String> reader = idReader(columnName, String.class);

        String id = table.idOf(timestamp);
        ResultSet resultSet = table.resultSetWithId(id);
        assertThat(resultSet.next())
                .isTrue();

        String acquiredId = reader.readValue(resultSet);
        assertThat(acquiredId)
                .isEqualTo(id);
    }

    @Test
    @DisplayName("read the value of an ID column of Message type")
    void readMessageId() throws SQLException {
        TimestampByMessage table = new TimestampByMessage(dataSource, H2_1_4);
        table.create();

        Timestamp timestamp = timestamp();
        table.write(timestamp);
        String columnName = table.idColumn()
                                 .columnName();
        ColumnReader<StringValue> reader = idReader(columnName, StringValue.class);

        StringValue id = table.idOf(timestamp);
        ResultSet resultSet = table.resultSetWithId(id);
        assertThat(resultSet.next())
                .isTrue();

        StringValue acquiredId = reader.readValue(resultSet);
        assertThat(acquiredId)
                .isEqualTo(id);
    }

    @Test
    @DisplayName("read the value of a column storing serialized messages")
    void readSerializedMessage() throws SQLException {
        TimestampByMessage table = new TimestampByMessage(dataSource, H2_1_4);
        table.create();

        Timestamp timestamp = timestamp();
        table.write(timestamp);
        String columnName = MessageTable.bytesColumn()
                                        .name();
        ColumnReader<Message> reader = messageReader(columnName, Timestamp.getDescriptor());

        StringValue id = table.idOf(timestamp);
        ResultSet resultSet = table.resultSet(id);
        assertThat(resultSet.next())
                .isTrue();

        Message acquiredValue = reader.readValue(resultSet);
        assertThat(acquiredValue)
                .isEqualTo(timestamp);
    }

    private static Timestamp timestamp() {
        return Timestamp
                .newBuilder()
                .setSeconds(1155000)
                .setNanos(15)
                .build();
    }
}
