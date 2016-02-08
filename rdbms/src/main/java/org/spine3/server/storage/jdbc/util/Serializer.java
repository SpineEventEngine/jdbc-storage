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

package org.spine3.server.storage.jdbc.util;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import org.spine3.Internal;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.type.TypeName;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.spine3.protobuf.Messages.fromAny;
import static org.spine3.protobuf.Messages.toAny;

/**
 * A utility class for serializing/deserializing storage records.
 *
 * @author Alexander Litus
 */
@Internal
@SuppressWarnings("UtilityClass")
public class Serializer {

    private Serializer() {}

    /**
     * Reads one storage record from a result set produced by a {@code statement} and deserializes it.
     *
     * @param statement a statement used to retrieve a result set
     * @param columnName a column name of a serialized (to bytes) storage record
     * @param recordDescriptor a descriptor of a storage record
     * @param <Record> a storage record type
     * @return a deserialized record
     */
    @Nullable
    public static <Record extends Message> Record readDeserializedRecord(PreparedStatement statement,
                                                                         String columnName,
                                                                         Descriptor recordDescriptor) {
        try (ResultSet resultSet = statement.executeQuery()) {
            if (!resultSet.next()) {
                return null;
            }
            final byte[] bytes = resultSet.getBytes(columnName);
            final Record record = deserializeRecord(bytes, recordDescriptor);
            return record;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    /**
     * Reads one storage record from a {@code resultSet} and deserializes it.
     *
     * @param resultSet a result set used to retrieve a serialized record
     * @param columnName a column name of a serialized (to bytes) storage record
     * @param recordDescriptor a descriptor of a storage record
     * @param <Record> a storage record type
     * @return a deserialized record
     */
    public static <Record extends Message> Record readDeserializedRecord(ResultSet resultSet,
                                                                         String columnName,
                                                                         Descriptor recordDescriptor) {
        try {
            final byte[] bytes = resultSet.getBytes(columnName);
            final Record record = deserializeRecord(bytes, recordDescriptor);
            return record;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    private static <Record extends Message> Record deserializeRecord(byte[] bytes, Descriptor recordDescriptor) {
        final Any.Builder builder = Any.newBuilder();
        final String typeUrl = TypeName.of(recordDescriptor).toTypeUrl();
        builder.setTypeUrl(typeUrl);
        final ByteString byteString = ByteString.copyFrom(bytes);
        builder.setValue(byteString);
        final Record record = fromAny(builder.build());
        return record;
    }

    /**
     * Serialized a record to an array of bytes.
     *
     * @param record a record to serialize
     * @param <Record> a storage record type
     * @return a byte array
     */
    public static <Record extends Message> byte[] serialize(Record record) {
        final Any any = toAny(record);
        final byte[] bytes = any.getValue().toByteArray();
        return bytes;
    }
}
