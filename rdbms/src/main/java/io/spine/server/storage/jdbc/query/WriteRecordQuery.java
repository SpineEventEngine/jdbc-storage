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

import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.type.JdbcColumnType;
import io.spine.server.storage.jdbc.util.ConnectionWrapper;
import io.spine.server.storage.jdbc.util.IdColumn;
import io.spine.server.storage.jdbc.util.Serializer;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public abstract class WriteRecordQuery<I, R> extends ColumnAwareWriteQuery {

    private final I id;
    private final EntityRecordWithColumns record;
    private final int idIndexInQuery;
    private final int recordIndexInQuery;
    private final IdColumn<I> idColumn;

    public EntityRecordWithColumns getRecord() {
        return record;
    }

    public I getId() {
        return id;
    }

    protected WriteRecordQuery(
            Builder<? extends Builder, ? extends WriteRecordQuery, I, R> builder) {
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
            final byte[] bytes = Serializer.serialize(record.getRecord());
            statement.setBytes(recordIndexInQuery, bytes);
            return statement;
        } catch (SQLException e) {
            logWriteError(id, e);
            throw new DatabaseException(e);
        }
    }

    @SuppressWarnings("ClassNameSameAsAncestorName")
    public abstract static class Builder<B extends Builder<B, Q, I, R>,
                                         Q extends WriteRecordQuery,
                                         I,
                                         R>
            extends ColumnAwareWriteQuery.Builder<B, Q> {

        private int idIndexInQuery;
        private int recordIndexInQuery;
        private IdColumn<I> idColumn;
        private I id;
        private EntityRecordWithColumns record;
        private ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> columnTypeRegistry;

        public B setColumnTypeRegistry(
                ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> columnTypeRegistry) {
            this.columnTypeRegistry = columnTypeRegistry;
            return getThis();
        }

        public B setId(I id) {
            this.id = id;
            return getThis();
        }

        public B setRecord(EntityRecordWithColumns record) {
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