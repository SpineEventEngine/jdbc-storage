/*
 * Copyright 2019, TeamDev. All rights reserved.
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

package io.spine.server.storage.jdbc.given.table;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Timestamp;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.given.query.SelectMessageId;
import io.spine.server.storage.jdbc.given.query.SelectTimestampById;
import io.spine.server.storage.jdbc.message.MessageTable;
import io.spine.server.storage.jdbc.query.IdColumn;

import java.sql.ResultSet;

import static io.spine.server.storage.jdbc.Type.INT;
import static io.spine.server.storage.jdbc.Type.LONG;

/**
 * Holds {@link Timestamp} records by some ID.
 *
 * <p>Overrides several {@link MessageTable} methods to expose them to tests.
 *
 * @param <I>
 *         the ID type
 */
abstract class TimestampTable<I> extends MessageTable<I, Timestamp> {

    TimestampTable(String name,
                   IdColumn<I> idColumn,
                   DataSourceWrapper dataSource,
                   TypeMapping typeMapping) {
        super(name, idColumn, dataSource, typeMapping);
    }

    @Override
    public void write(Timestamp record) {
        super.write(record);
    }

    @Override
    public ResultSet resultSet(I id) {
        return super.resultSet(id);
    }

    @Override
    public ResultSet resultSet(Iterable<I> ids) {
        return super.resultSet(ids);
    }

    @Override
    public I idOf(Timestamp record) {
        return super.idOf(record);
    }

    @Override
    protected Descriptor messageDescriptor() {
        return Timestamp.getDescriptor();
    }

    @SuppressWarnings("unchecked") // Ensured by class descendants.
    @Override
    protected Iterable<? extends MessageTable.Column<Timestamp>> messageSpecificColumns() {
        MessageTable.Column<Timestamp> idColumn =
                (MessageTable.Column<Timestamp>) idColumn().column();
        return ImmutableSet.of(idColumn, Column.SECONDS, Column.NANOS);
    }

    /**
     * Reads a given ID back from the database as a {@link ResultSet}.
     */
    public ResultSet resultSetWithId(I id) {
        SelectMessageId.Builder<I, Timestamp> queryBuilder = SelectMessageId
                .<I, Timestamp>newBuilder()
                .setDataSource(dataSource())
                .setTableName(name())
                .setIdColumn(idColumn())
                .setId(id);
        SelectMessageId<I, Timestamp> query = queryBuilder.build();
        ResultSet resultSet = query.getResults();
        return resultSet;
    }

    /**
     * Composes a "select-timestamp-by-ID" query.
     */
    public SelectTimestampById<I> composeSelectTimestampById(I id) {
        SelectTimestampById.Builder<I> builder = SelectTimestampById
                .<I>newBuilder()
                .setDataSource(dataSource())
                .setTableName(name())
                .setMessageColumnName(bytesColumn().name())
                .setMessageDescriptor(Timestamp.getDescriptor())
                .setIdColumn(idColumn())
                .setId(id);
        SelectTimestampById<I> query = builder.build();
        return query;
    }

    public enum Column implements MessageTable.Column<Timestamp> {
        SECONDS(LONG, Timestamp::getSeconds),
        NANOS(INT, Timestamp::getNanos);

        private final Type type;
        private final Getter<Timestamp> getter;

        Column(Type type, Getter<Timestamp> getter) {
            this.type = type;
            this.getter = getter;
        }

        @Override
        public Getter<Timestamp> getter() {
            return getter;
        }

        @Override
        public Type type() {
            return type;
        }

        @Override
        public boolean isPrimaryKey() {
            return false;
        }

        @Override
        public boolean isNullable() {
            return false;
        }
    }
}
