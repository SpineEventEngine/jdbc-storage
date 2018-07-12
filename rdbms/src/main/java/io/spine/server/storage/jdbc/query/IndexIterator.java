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

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.json.Json.fromJson;
import static io.spine.util.Exceptions.newIllegalArgumentException;

/**
 * An iterator over the IDs of a table.
 *
 * @author Dmytro Dashenkov
 */
abstract class IndexIterator<I> extends DbIterator<I> {

    /**
     * Creates a new iterator instance.
     *
     * @param resultSet  a result set of IDs (will be closed on a {@link #close()})
     * @param columnName a name of a serialized storage record column
     */
    private IndexIterator(ResultSet resultSet, String columnName) {
        super(resultSet, columnName);
    }

    /**
     * Creates a new iterator instance.
     *
     * @param resultSet  a result set of IDs (will be closed on a {@link #close()})
     * @param columnName a name of a serialized storage record column
     */
    @SuppressWarnings("unchecked") // Logically checked by if statements
    static <I> IndexIterator<I> create(ResultSet resultSet, String columnName, Class<I> idType) {
        checkNotNull(resultSet);
        checkNotNull(columnName);
        checkNotNull(idType);
        Class<I> wrapper = Primitives.wrap(idType);
        IndexIterator<I> result;
        if (String.class.equals(idType)) {
            result = (IndexIterator<I>) new StringIndexIterator(resultSet, columnName);
        } else if (Integer.class == wrapper || Long.class == wrapper) {
            result = (IndexIterator<I>) new NumberIndexIterator(resultSet, columnName);
        } else if (Message.class.isAssignableFrom(idType)) {
            result = (IndexIterator<I>) new MessageIndexIterator(resultSet, columnName, idType);
        } else {
            throw newIllegalArgumentException("ID type '%s' is not supported.", idType);
        }
        return result;
    }

    private static class StringIndexIterator extends IndexIterator<String> {

        /**
         * Creates a new iterator instance.
         *
         * @param resultSet  a result set of IDs (will be closed on a {@link #close()})
         * @param columnName a name of a serialized storage record column
         */
        private StringIndexIterator(ResultSet resultSet, String columnName) {
            super(resultSet, columnName);
        }

        @Override
        protected String readResult() throws SQLException {
            String result = getResultSet().getString(getColumnName());
            return result;
        }
    }

    private static class NumberIndexIterator extends IndexIterator<Number> {

        /**
         * Creates a new iterator instance.
         *
         * @param resultSet  a result set of IDs (will be closed on a {@link #close()})
         * @param columnName a name of a serialized storage record column
         */
        private NumberIndexIterator(ResultSet resultSet, String columnName) {
            super(resultSet, columnName);
        }

        @Override
        protected Number readResult() throws SQLException {
            Number result = getResultSet().getLong(getColumnName());
            return result;
        }
    }

    private static class MessageIndexIterator<M extends Message> extends IndexIterator<M> {

        private final Class<M> idClass;

        /**
         * Creates a new iterator instance.
         *
         * @param resultSet  a result set of IDs (will be closed on a {@link #close()})
         * @param columnName a name of a serialized storage record column
         */
        private MessageIndexIterator(ResultSet resultSet,
                                     String columnName,
                                     Class<M> idClass) {
            super(resultSet, columnName);
            this.idClass = idClass;
        }

        @Override
        protected M readResult() throws SQLException {
            String rawId = getResultSet().getString(getColumnName());
            M messageId = fromJson(rawId, idClass);
            return messageId;
        }
    }
}
