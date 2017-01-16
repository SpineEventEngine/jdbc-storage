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

package org.spine3.server.storage.jdbc.query;

import com.google.protobuf.Message;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;
import org.spine3.server.storage.jdbc.util.Serializer;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public abstract class WriteRecordQuery<I, Record extends Message> extends WriteQuery{

    private final I id;
    private final Record record;
    private final int idIndexInQuery;
    private final int recordIndexInQuery;
    private final IdColumn<I> idColumn;

    public Record getRecord() {
        return record;
    }

    protected WriteRecordQuery(Builder<? extends Builder, ? extends WriteRecordQuery, I, Record> builder) {
        super(builder);
        this.idIndexInQuery = builder.idIndexInQuery;
        this.recordIndexInQuery = builder.recordIndexInQuery;
        this.idColumn = builder.idColumn;
        this.id = builder.id;
        this.record = builder.record;
    }

    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        final PreparedStatement statement = super.prepareStatement(connection);
        try {
            idColumn.setId(idIndexInQuery, id, statement);
            final byte[] bytes = Serializer.serialize(record);
            statement.setBytes(recordIndexInQuery, bytes);
            return statement;
        } catch (SQLException e) {
            getLogger().error("Failed to write record with id " + id, e);
            throw new DatabaseException(e);
        }
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public abstract static class Builder<B extends Builder<B, Q, I, Record>, Q extends WriteRecordQuery, I, Record extends Message>
            extends WriteQuery.Builder<B, Q>{
        private int idIndexInQuery;
        private int recordIndexInQuery;
        private IdColumn<I> idColumn;
        private I id;
        private Record record;


        public B setId(I id) {
            this.id = id;
            return getThis();
        }

        public B setRecord(Record record) {
            this.record = record;
            return getThis();
        }

        public B setIdColumn(IdColumn<I> idColumn) {
            this.idColumn = idColumn;
            return getThis();
        }

        public B setIdIndexInQuery(int idIndexInQuery) {
            this.idIndexInQuery = idIndexInQuery;
            return getThis();
        }

        public B setRecordIndexInQuery(int recordIndexInQuery) {
            this.recordIndexInQuery = recordIndexInQuery;
            return getThis();
        }
    }
}
