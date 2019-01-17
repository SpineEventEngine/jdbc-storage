/*
 * Copyright 2019, TeamDev. All rights reserved.
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

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.server.storage.jdbc.query.Serializer.deserialize;

/**
 * The reader for the columns which store Protobuf messages in a serialized form.
 *
 * <p>The result of the read operation is a deserialized {@link Message}.
 *
 * @param <M>
 *         the type of the messages stored in the column
 */
final class MessageBytesColumnReader<M extends Message> extends ColumnReader<M> {

    private final Descriptor messageDescriptor;

    private MessageBytesColumnReader(String columnName, Descriptor messageDescriptor) {
        super(columnName);
        this.messageDescriptor = messageDescriptor;
    }

    /**
     * Creates a new instance of the {@code MessageBytesColumnReader}.
     *
     * @param columnName
     *         the name of the column to read
     * @param messageDescriptor
     *         the {@code Descriptor} of the column message type
     */
    static <M extends Message> MessageBytesColumnReader<M>
    create(String columnName, Descriptor messageDescriptor) {
        return new MessageBytesColumnReader<>(columnName, messageDescriptor);
    }

    @Override
    public M readValue(ResultSet resultSet) throws SQLException {
        checkNotNull(resultSet);
        byte[] bytes = resultSet.getBytes(columnName());
        M result = deserialize(bytes, messageDescriptor);
        return result;
    }
}
