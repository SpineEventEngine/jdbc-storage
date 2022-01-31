/*
 * Copyright 2022, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.query.reader;

import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import io.spine.server.storage.jdbc.JdbcStorageFactory;
import io.spine.server.storage.jdbc.given.table.TimestampByLong;
import io.spine.server.storage.jdbc.given.table.TimestampByMessage;
import io.spine.server.storage.jdbc.given.table.TimestampByString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static com.google.common.truth.Truth.assertThat;
import static io.spine.base.Identifier.newUuid;
import static io.spine.server.storage.jdbc.GivenDataSource.whichIsStoredInMemory;
import static io.spine.server.storage.jdbc.PredefinedMapping.H2_2_1;
import static io.spine.server.storage.jdbc.query.reader.ColumnReaderFactory.idReader;
import static io.spine.server.storage.jdbc.query.reader.ColumnReaderFactory.messageReader;
import static io.spine.server.storage.jdbc.record.column.BytesColumn.bytesColumnName;

@DisplayName("`ColumnReader` should")
class ColumnReaderTest {

    private JdbcStorageFactory factory;

    @BeforeEach
    void setUpDataSource() {
        var dataSource = whichIsStoredInMemory(newUuid());
        factory = JdbcStorageFactory.newBuilder()
                .setDataSource(dataSource)
                .setTypeMapping(H2_2_1)
                .build();
    }

    @Test
    @DisplayName("read the value of an ID column of `Number` type")
    void readNumberId() throws SQLException {
        var table = new TimestampByLong(factory);
        table.create();

        var timestamp = timestamp();
        table.write(timestamp);
        var columnName = table.idColumn()
                              .columnName();
        var reader = idReader(columnName, Long.class);

        var id = table.idOf(timestamp);
        var resultSet = table.resultSetWithId(id);
        assertThat(resultSet.next())
                .isTrue();

        var acquiredId = reader.readValue(resultSet);
        assertThat(acquiredId)
                .isEqualTo(id);
    }

    @Test
    @DisplayName("read the value of an ID column of `String` type")
    void readStringId() throws SQLException {
        var table = new TimestampByString(factory);
        table.create();

        var timestamp = timestamp();
        table.write(timestamp);
        var columnName = table.idColumn().columnName();
        var reader = idReader(columnName, String.class);

        var id = table.idOf(timestamp);
        var resultSet = table.resultSetWithId(id);
        assertThat(resultSet.next())
                .isTrue();

        var acquiredId = reader.readValue(resultSet);
        assertThat(acquiredId)
                .isEqualTo(id);
    }

    @Test
    @DisplayName("read the value of an ID column of Message type")
    void readMessageId() throws SQLException {
        var table = new TimestampByMessage(factory);
        table.create();

        var timestamp = timestamp();
        table.write(timestamp);
        var columnName = table.idColumn()
                              .columnName();
        var reader = idReader(columnName, StringValue.class);

        var id = table.idOf(timestamp);
        var resultSet = table.resultSetWithId(id);
        assertThat(resultSet.next())
                .isTrue();

        var acquiredId = reader.readValue(resultSet);
        assertThat(acquiredId)
                .isEqualTo(id);
    }

    @Test
    @DisplayName("read the value of a column storing serialized messages")
    void readSerializedMessage() throws SQLException {
        var table = new TimestampByMessage(factory);
        table.create();

        var timestamp = timestamp();
        table.write(timestamp);
        ColumnReader<Message> reader = messageReader(bytesColumnName(), Timestamp.getDescriptor());

        var id = table.idOf(timestamp);
        var resultSet = table.resultSet(id);
        assertThat(resultSet.next())
                .isTrue();

        var acquiredValue = reader.readValue(resultSet);
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
