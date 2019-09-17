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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Timestamp;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.message.MessageTable;
import io.spine.server.storage.jdbc.query.IdColumn;

import java.sql.ResultSet;

import static io.spine.server.storage.jdbc.Type.INT;
import static io.spine.server.storage.jdbc.Type.LONG;

/**
 * Holds {@link Timestamp} records by {@code Long} IDs.
 */
public final class TimestampByLong extends MessageTable<Long, Timestamp> {

    private static final String NAME = "timestamp_by_long";

    public TimestampByLong(DataSourceWrapper dataSource, TypeMapping typeMapping) {
        super(NAME, IdColumn.of(Column.ID), dataSource, typeMapping);
    }

    /**
     * Reads a given ID back from the database as a {@link ResultSet}.
     *
     * <p>Allows to obtain a {@link ResultSet} with a message ID which is sometimes necessary in
     * tests.
     */
    public ResultSet resultSetWithId(Long id) {
        SelectMessageId.Builder<Long, Timestamp> queryBuilder = SelectMessageId
                .<Long, Timestamp>newBuilder()
                .setDataSource(dataSource())
                .setTableName(name())
                .setIdColumn(idColumn())
                .setId(id);
        SelectMessageId<Long, Timestamp> query = queryBuilder.build();
        ResultSet resultSet = query.query()
                                   .getResults();
        return resultSet;
    }

    @Override
    public Long idOf(Timestamp record) {
        return super.idOf(record);
    }

    @Override
    protected Descriptor messageDescriptor() {
        return Timestamp.getDescriptor();
    }

    @Override
    protected Iterable<Column> messageSpecificColumns() {
        return ImmutableList.copyOf(Column.values());
    }

    enum Column implements MessageTable.Column<Timestamp> {
        ID(LONG, Column::idOf),
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
            return this == ID;
        }

        @Override
        public boolean isNullable() {
            return false;
        }

        private static long idOf(Timestamp timestamp) {
            long id = timestamp.getSeconds() + timestamp.getNanos();
            return id;
        }
    }
}
