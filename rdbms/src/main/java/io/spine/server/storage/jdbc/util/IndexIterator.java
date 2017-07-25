/*
 * Copyright 2017, TeamDev Ltd. All rights reserved.
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

package io.spine.server.storage.jdbc.util;

import com.google.common.primitives.Primitives;
import com.google.protobuf.Message;
import io.spine.annotation.Internal;
import io.spine.server.storage.jdbc.DatabaseException;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.json.Json.fromJson;
import static io.spine.util.Exceptions.newIllegalArgumentException;

/**
 * An iterator over the IDs of a table.
 *
 * @author Dmytro Dashenkov
 */
@Internal
public abstract class IndexIterator<I> extends DbIterator<I> {

    /**
     * Creates a new iterator instance.
     *
     * @param statement  a statement used to retrieve a result set
     *                   (both statement and result set are closed in {@link #close()}).
     * @param columnName a name of a serialized storage record column
     * @throws DatabaseException if an error occurs during interaction with the DB
     */
    private IndexIterator(PreparedStatement statement, String columnName) throws DatabaseException {
        super(statement, columnName);
    }

    /**
     * Creates a new iterator instance.
     *
     * @param statement  a statement used to retrieve a result set
     *                   (both statement and result set are closed in {@link #close()}).
     * @param columnName a name of a serialized storage record column
     * @throws DatabaseException if an error occurs during interaction with the DB
     */
    @SuppressWarnings("unchecked") // Logically checked by if statements
    public static <I> IndexIterator<I> create(PreparedStatement statement,
                                              String columnName,
                                              Class<I> idType)
            throws DatabaseException {
        checkNotNull(statement);
        checkNotNull(columnName);
        checkNotNull(idType);
        final Class<I> wrapper = Primitives.wrap(idType);
        final IndexIterator<I> result;
        if (String.class.equals(idType)) {
            result = (IndexIterator<I>) new StringIndexIterator(statement, columnName);
        } else if (Integer.class == wrapper || Long.class == wrapper) {
            result = (IndexIterator<I>) new NumberIndexIterator(statement, columnName);
        } else if (Message.class.isAssignableFrom(idType)) {
            result = (IndexIterator<I>) new MessageIndexIterator(statement, columnName, idType);
        } else {
            throw newIllegalArgumentException("ID type '%s' is not supported.", idType);
        }
        return result;
    }

    private static class StringIndexIterator extends IndexIterator<String> {

        /**
         * Creates a new iterator instance.
         *
         * @param statement  a statement used to retrieve a result set
         *                   (both statement and result set are closed in {@link #close()}).
         * @param columnName a name of a serialized storage record column
         * @throws DatabaseException if an error occurs during interaction with the DB
         */
        private StringIndexIterator(PreparedStatement statement, String columnName)
                throws DatabaseException {
            super(statement, columnName);
        }

        @Override
        protected String readResult() throws SQLException {
            final String result = getResultSet().getString(getColumnName());
            return result;
        }
    }

    private static class NumberIndexIterator extends IndexIterator<Number> {

        /**
         * Creates a new iterator instance.
         *
         * @param statement  a statement used to retrieve a result set
         *                   (both statement and result set are closed in {@link #close()}).
         * @param columnName a name of a serialized storage record column
         * @throws DatabaseException if an error occurs during interaction with the DB
         */
        private NumberIndexIterator(PreparedStatement statement, String columnName)
                throws DatabaseException {
            super(statement, columnName);
        }

        @Override
        protected Number readResult() throws SQLException {
            final Number result = getResultSet().getLong(getColumnName());
            return result;
        }
    }

    private static class MessageIndexIterator<M extends Message> extends IndexIterator<M> {

        private final Class<M> idClass;

        /**
         * Creates a new iterator instance.
         *
         * @param statement  a statement used to retrieve a result set
         *                   (both statement and result set are closed in {@link #close()}).
         * @param columnName a name of a serialized storage record column
         * @throws DatabaseException if an error occurs during interaction with the DB
         */
        private MessageIndexIterator(PreparedStatement statement,
                                     String columnName,
                                     Class<M> idClass)
                throws DatabaseException {
            super(statement, columnName);
            this.idClass = idClass;
        }

        @Override
        protected M readResult() throws SQLException {
            final String rawId = getResultSet().getString(getColumnName());
            final M messageId = fromJson(rawId, idClass);
            return messageId;
        }
    }
}
