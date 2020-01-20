/*
 * Copyright 2020, TeamDev. All rights reserved.
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
import io.spine.annotation.Internal;

/**
 * The factory which creates {@link ColumnReader} instances.
 */
@Internal
public final class ColumnReaderFactory {

    /** Prevents instantiation of this utility class. */
    private ColumnReaderFactory() {
    }

    /**
     * Creates a reader for the column storing index values.
     *
     * <p>The index values are stored in the DB differently from the other column values, for
     * example a Protobuf {@link Message} is serialized to JSON instead of {@code bytes}.
     *
     * @param columnName
     *         the name of the column to create the reader for
     * @param idType
     *         the class of the IDs stored by the column
     * @param <I>
     *         the compile-time type of the IDs stored in the column
     * @return the {@code ColumnReader} instance for the given column
     */
    public static <I> ColumnReader<I> idReader(String columnName, Class<I> idType) {
        return IndexColumnReaders.create(columnName, idType);
    }

    /**
     * Creates a reader for the column storing serialized Protobuf {@linkplain Message messages}.
     *
     * @param columnName
     *         the name of the column to create the reader for
     * @param messageDescriptor
     *         the descriptor of the column message type
     * @param <M>
     *         the compile-time type of the messages stored in the column
     * @return the {@code ColumnReader} for the given column
     */
    public static <M extends Message> MessageBytesColumnReader<M>
    messageReader(String columnName, Descriptor messageDescriptor) {
        return MessageBytesColumnReader.create(columnName, messageDescriptor);
    }

    public static ColumnReader<Integer> intReader(String columnName) {
        return new IntegerColumnReader(columnName);
    }
}
