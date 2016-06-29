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

package org.spine3.server.storage.jdbc.query;


import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.util.ConnectionWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;
import org.spine3.server.storage.jdbc.util.Serializer;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public abstract class WriteRecord<Id, Record extends Message> extends AbstractQuery implements Write{

    private final Id id;
    private final Record record;

    private int idIndexInQuery;
    private int recordIndexInQuery;
    private final IdColumn<Id> idColumn;

    private String messageColumnName;
    private Descriptors.Descriptor messageDescriptor;

    protected WriteRecord(Builder<? extends Builder, ? extends WriteRecord, Id, Record> builder) {
        super(builder);
        this.idIndexInQuery = builder.idIndexInQuery;
        this.recordIndexInQuery = builder.recordIndexInQuery;
        this.idColumn = builder.idColumn;
        this.id = builder.id;
        this.record = builder.record;
    }

    public void execute() throws DatabaseException {
        try (ConnectionWrapper connection = this.dataSource.getConnection(false)) {
            try (PreparedStatement statement = prepareStatement(connection)) {
                statement.execute();
                connection.commit();
            } catch (SQLException e) {
                // logError(e);
                connection.rollback();
                throw new DatabaseException(e);
            }
        }
    }

    public void setMessageColumnName(String messageColumnName) {
        this.messageColumnName = messageColumnName;
    }

    public void setMessageDescriptor(Descriptors.Descriptor messageDescriptor) {
        this.messageDescriptor = messageDescriptor;
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
            throw new DatabaseException(e);
        }
    }

    public abstract static class Builder<B extends Builder<B, Q, Id, Record>, Q extends WriteRecord, Id, Record extends Message>
            extends AbstractQuery.Builder<B, Q>{
        private int idIndexInQuery;
        private int recordIndexInQuery;
        private IdColumn<Id> idColumn;
        private Id id;
        private Record record;


        public B setId(Id id) {
            this.id = id;
            return getThis();
        }

        public B setRecord(Record record) {
            this.record = record;
            return getThis();
        }

        public B setIdColumn(IdColumn<Id> idColumn) {
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
