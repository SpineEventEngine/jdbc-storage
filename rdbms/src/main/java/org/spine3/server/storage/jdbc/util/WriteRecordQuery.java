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

import com.google.protobuf.Message;
import org.spine3.Internal;
import org.spine3.server.storage.jdbc.DatabaseException;

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
public abstract class WriteRecordQuery<Id, Record extends Message> extends WriteQuery {

    private final String query;
    private final int idIndexInQuery;
    private final int recordIndexInQuery;
    private final IdColumn<Id> idColumn;
    private final Id id;
    private final Record record;

    /**
     * Creates a new query instance based on the passed builder.
     */
    protected WriteRecordQuery(AbstractBuilder<? extends WriteRecordQuery<Id, Record>, Id, Record> builder) {
        super(builder.dataSource);
        this.query = builder.query;
        this.idIndexInQuery = builder.idIndexInQuery;
        this.recordIndexInQuery = builder.recordIndexInQuery;
        this.idColumn = builder.idColumn;
        this.id = builder.id;
        this.record = builder.record;
    }

    @Override
    protected PreparedStatement prepareStatement(ConnectionWrapper connection) {
        try {
            final PreparedStatement statement = connection.prepareStatement(query);
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

    /**
     * A base for query builders.
     *
     * @param <Query> a type of query to build
     * @param <Id> a type of record IDs
     * @param <Record> a type of records to write
     */
    public abstract static class AbstractBuilder<Query extends WriteRecordQuery<Id, Record>, Id, Record extends Message> {

        private DataSourceWrapper dataSource;
        private String query;
        private int idIndexInQuery;
        private int recordIndexInQuery;
        private IdColumn<Id> idColumn;
        private Id id;
        private Record record;

        public abstract Query build();

        public AbstractBuilder<Query, Id, Record> setDataSource(DataSourceWrapper dataSource) {
            this.dataSource = dataSource;
            return this;
        }

        public AbstractBuilder<Query, Id, Record> setQuery(String query) {
            this.query = query;
            return this;
        }

        public AbstractBuilder<Query, Id, Record> setIdIndexInQuery(int idIndexInQuery) {
            this.idIndexInQuery = idIndexInQuery;
            return this;
        }

        public AbstractBuilder<Query, Id, Record> setRecordIndexInQuery(int recordIndexInQuery) {
            this.recordIndexInQuery = recordIndexInQuery;
            return this;
        }

        public AbstractBuilder<Query, Id, Record> setIdColumn(IdColumn<Id> idColumn) {
            this.idColumn = idColumn;
            return this;
        }

        public AbstractBuilder<Query, Id, Record> setId(Id id) {
            this.id = id;
            return this;
        }

        public AbstractBuilder<Query, Id, Record> setRecord(Record record) {
            this.record = record;
            return this;
        }
    }
}
