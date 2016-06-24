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

import com.google.protobuf.Message;
import org.spine3.Internal;
import org.spine3.server.storage.jdbc.DatabaseException;
import org.spine3.server.storage.jdbc.util.*;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * A query which is executed in order to write a {@link Message} record to the data source.
 *
 * @param <Id> a type of record IDs
 * @param <Record> a type of records to write
 * @author Alexander Litus
 */
@Internal
public abstract class WriteRecord<Id, Record extends Message> extends Write {

    private final Id id;
    private final Record record;

    private final int idIndexInQuery;
    private final int recordIndexInQuery;
    private final IdColumn<Id> idColumn;

    /**
     * Creates a new query instance based on the passed builder.
     */
    protected WriteRecord(Builder<? extends Builder, ? extends WriteRecord<Id, Record>, Id, Record> builder) {
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
            throw new DatabaseException(e);
        }
    }

    protected Id getId() {
        return id;
    }


    protected abstract static class Builder<B extends Builder<B, Q, Id, Record>, Q extends WriteRecord<Id, Record>, Id, Record extends Message>
            extends Write.Builder<B, Q>{

        private int idIndexInQuery;
        private int recordIndexInQuery;
        private IdColumn<Id> idColumn;
        private Id id;
        private Record record;


        public Builder<B, Q, Id, Record> setIdIndexInQuery(int idIndexInQuery) {
            this.idIndexInQuery = idIndexInQuery;
            return getThis();
        }

        public Builder<B, Q, Id, Record> setRecordIndexInQuery(int recordIndexInQuery) {
            this.recordIndexInQuery = recordIndexInQuery;
            return getThis();
        }

        public Builder<B, Q, Id, Record> setIdColumn(IdColumn<Id> idColumn) {
            this.idColumn = idColumn;
            return getThis();
        }

        public Builder<B, Q, Id, Record> setId(Id id) {
            this.id = id;
            return getThis();
        }

        public Builder<B, Q, Id, Record> setRecord(Record record) {
            this.record = record;
            return getThis();
        }
    }
}
