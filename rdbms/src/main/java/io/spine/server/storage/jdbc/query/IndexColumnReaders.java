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

import com.google.common.primitives.Primitives;
import com.google.protobuf.Message;

import static io.spine.util.Exceptions.newIllegalArgumentException;

/**
 * A helper class that allows to distinguish between the different {@link ColumnReader} types for
 * the different ID types.
 */
final class IndexColumnReaders {

    private IndexColumnReaders() {
    }

    /**
     * Creates a new iterator for the column storing entity IDs.
     *
     * @param columnName
     *         the name of the ID column
     * @param idType
     *         the type of the IDs stored in column
     * @param <I>
     *         the compile-time type of the IDs
     * @return a new instance of {@code ColumnReader}
     */
    @SuppressWarnings({"unchecked" /* Logically checked by if statements. */,
            "IfStatementWithTooManyBranches" /* Required to differentiate between reader types. */})
    static <I> ColumnReader<I> create(String columnName, Class<I> idType) {
        Class<I> wrapper = Primitives.wrap(idType);
        if (String.class.equals(idType)) {
            return (ColumnReader<I>) new StringColumnReader(columnName);
        } else if (Integer.class == wrapper || Long.class == wrapper) {
            return (ColumnReader<I>) new NumberColumnReader(columnName);
        } else if (Message.class.isAssignableFrom(idType)) {
            return new MessageColumnReader(columnName, idType);
        } else {
            throw newIllegalArgumentException("ID type '%s' is not supported.", idType);
        }
    }
}
