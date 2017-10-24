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
package io.spine.server.storage.jdbc.query;

import com.google.protobuf.Message;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.ConnectionWrapper;
import io.spine.server.storage.jdbc.IdColumn;
import io.spine.server.storage.jdbc.Serializer;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static io.spine.server.storage.jdbc.AggregateEventRecordTable.Column.aggregate;

/**
 * An abstract base for the write queries to an {@link io.spine.server.storage.jdbc.AggregateTable}.
 *
 * @author Alexander Aleksandrov
 */
abstract class WriteAggregateQuery<I, R extends Message> extends WriteQuery {

    private final I id;
    private final R record;
    private final IdColumn<I, ?> idColumn;

    R getRecord() {
        return record;
    }

    WriteAggregateQuery(Builder<? extends Builder, ? extends WriteAggregateQuery, I, R> builder) {
        super(builder);
        this.idColumn = builder.idColumn;
        this.id = builder.id;
        this.record = builder.record;
    }

    //TODO:2017-10-23:dmytro.grankin: remove after reworking of WriteQuery.execute().
    @Override
    public void execute() {
        try (ConnectionWrapper connection = getConnection(false)) {
            try (PreparedStatement statement = prepareStatementWithParameters(connection)) {
                statement.execute();
                connection.commit();
            } catch (SQLException e) {
                getLogger().error("Failed to execute write operation.", e);
                connection.rollback();
                throw new DatabaseException(e);
            }
        }
    }

    @Override
    protected IdentifiedParameters getIdentifiedParameters() {
        final IdentifiedParameters superParameters = super.getIdentifiedParameters();
        final String recordName = aggregate.name();
        final byte[] serializedRecord = Serializer.serialize(record);
        return IdentifiedParameters.newBuilder()
                                   .addParameters(superParameters)
                                   .addParameter(idColumn.getColumnName(), idColumn.normalize(id))
                                   .addParameter(recordName, serializedRecord)
                                   .build();
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    abstract static class Builder<B extends Builder<B, Q, I, R>,
                                  Q extends WriteAggregateQuery,
                                  I,
                                  R extends Message>
            extends WriteQuery.Builder<B, Q> {

        private IdColumn<I, ?> idColumn;
        private I id;
        private R record;

        public B setId(I id) {
            this.id = id;
            return getThis();
        }

        public B setRecord(R record) {
            this.record = record;
            return getThis();
        }

        public B setIdColumn(IdColumn<I, ?> idColumn) {
            this.idColumn = idColumn;
            return getThis();
        }
    }
}
