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

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.server.storage.jdbc.query.ColumnReaderFactory.idReader;
import static io.spine.server.storage.jdbc.query.ColumnReaderFactory.messageReader;

public class IndexWithMessageIterator<I, M extends Message>
        extends DbIterator<IdWithMessage<I, M>> {

    private final String idColumnName;
    private final Class<I> idType;
    private final Descriptor messageDescriptor;

    /**
     * Creates a new iterator instance.
     * @param resultSet  the results of a DB query to iterate over
     * @param columnName a name of a serialized storage record column
     * @param idColumnName
     * @param idType
     * @param messageDescriptor
     */
    private IndexWithMessageIterator(ResultSet resultSet,
                                     String columnName,
                                     String idColumnName,
                                     Class<I> idType,
                                     Descriptor messageDescriptor) {
        super(resultSet, columnName);
        this.idColumnName = idColumnName;
        this.idType = idType;
        this.messageDescriptor = messageDescriptor;
    }

    @Override
    protected IdWithMessage<I, M> readResult() throws SQLException {
        ColumnReader<I> idReader = idReader(idColumnName, idType);
        ColumnReader<M> messageReader = messageReader(getColumnName(), messageDescriptor);
        I id = idReader.read(getResultSet());
        M message = messageReader.read(getResultSet());
        IdWithMessage<I, M> result = new IdWithMessage<>(id, message);
        return result;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private ResultSet resultSet;
        private String columnName;
        private String idColumnName;
        private Class<?> idType;
        private Descriptor messageDescriptor;

        public Builder setResultSet(ResultSet resultSet) {
            this.resultSet = resultSet;
            return this;
        }

        public Builder setColumnName(String columnName) {
            this.columnName = columnName;
            return this;
        }

        public Builder setIdColumnName(String idColumnName) {
            this.idColumnName = idColumnName;
            return this;
        }

        public Builder setIdType(Class<?> idType) {
            this.idType = idType;
            return this;
        }

        public Builder setMessageDescriptor(Descriptor messageDescriptor) {
            this.messageDescriptor = messageDescriptor;
            return this;
        }

        @SuppressWarnings("unchecked")
        // It's up to user to specify correct ID type and message descriptor.
        public IndexWithMessageIterator build() {
            checkNotNull(resultSet, "Result set not specified for IndexWithMessageIterator");
            checkNotNull(columnName, "Column name not specified for IndexWithMessageIterator");
            checkNotNull(idType, "ID type not specified for IndexWithMessageIterator");
            checkNotNull(messageDescriptor,
                         "Message descriptor not specified for IndexWithMessageIterator");
            return new IndexWithMessageIterator(resultSet, columnName, idColumnName, idType,
                                                messageDescriptor);
        }
    }
}
