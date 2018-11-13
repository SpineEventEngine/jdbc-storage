/*
 * Copyright 2018, TeamDev. All rights reserved.
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

import com.google.protobuf.Int32Value;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static io.spine.json.Json.toCompactJson;
import static io.spine.server.storage.jdbc.query.ColumnReaderFactory.idColumn;
import static io.spine.server.storage.jdbc.query.ColumnReaderFactory.messageColumn;
import static io.spine.server.storage.jdbc.query.Serializer.serialize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("ColumnReader should")
class ColumnReaderTest {

    private static final String COLUMN_NAME = "column_name";

    @Test
    @DisplayName("read value of ID column of Number type")
    void readNumberId() throws SQLException {
        ResultSet resultSet = mock(ResultSet.class);
        Long id = 42L;
        when(resultSet.getLong(COLUMN_NAME)).thenReturn(id);
        ColumnReader<Long> reader = idColumn(COLUMN_NAME, Long.class);
        Long acquiredId = reader.readValue(resultSet);
        assertEquals(id, acquiredId);
    }

    @Test
    @DisplayName("read value of ID column of String type")
    void readStringId() throws SQLException {
        ResultSet resultSet = mock(ResultSet.class);
        String id = "theString";
        when(resultSet.getString(COLUMN_NAME)).thenReturn(id);
        ColumnReader<String> reader = idColumn(COLUMN_NAME, String.class);
        String acquiredId = reader.readValue(resultSet);
        assertEquals(id, acquiredId);
    }

    @Test
    @DisplayName("read value of ID column of Message type")
    void readMessageId() throws SQLException {
        ResultSet resultSet = mock(ResultSet.class);
        StringValue id = StringValue
                .newBuilder()
                .setValue("stringValue")
                .build();
        String idJson = toCompactJson(id);
        when(resultSet.getString(COLUMN_NAME)).thenReturn(idJson);
        ColumnReader<StringValue> reader = idColumn(COLUMN_NAME, StringValue.class);
        StringValue acquiredId = reader.readValue(resultSet);
        assertEquals(id, acquiredId);
    }

    @Test
    @DisplayName("read value of column storing serialized messages")
    void readSerializedMessage() throws SQLException {
        ResultSet resultSet = mock(ResultSet.class);
        Int32Value columnValue = Int32Value
                .newBuilder()
                .setValue(42)
                .build();
        byte[] serializedValue = serialize(columnValue);
        when(resultSet.getBytes(COLUMN_NAME)).thenReturn(serializedValue);
        ColumnReader<Message> reader = messageColumn(COLUMN_NAME, Int32Value.getDescriptor());
        Message acquiredValue = reader.readValue(resultSet);
        assertEquals(columnValue, acquiredValue);
    }
}
