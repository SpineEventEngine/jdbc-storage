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
 * TODO:2015-12-08:alexander.litus: docs
 *
 * @author Alexander Litus
 */
@Internal
@SuppressWarnings("UtilityClass")
public class Serializer {

    private Serializer() {}

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

    @Nullable
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

    public static <Record extends Message> Record deserializeRecord(byte[] bytes, Descriptor recordDescriptor) {
        final Any.Builder builder = Any.newBuilder();
        final String typeUrl = TypeName.of(recordDescriptor).toTypeUrl();
        builder.setTypeUrl(typeUrl);
        final ByteString byteString = ByteString.copyFrom(bytes);
        builder.setValue(byteString);
        final Record record = fromAny(builder.build());
        return record;
    }

    public static <Record extends Message> byte[] serialize(Record record) {
        final Any any = toAny(record);
        final byte[] bytes = any.getValue().toByteArray();
        return bytes;
    }
}
