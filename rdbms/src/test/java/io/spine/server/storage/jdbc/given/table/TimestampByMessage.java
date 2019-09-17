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
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.message.MessageTable;
import io.spine.server.storage.jdbc.query.IdColumn;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.ResultSet;

import static io.spine.server.storage.jdbc.Type.INT;
import static io.spine.server.storage.jdbc.Type.LONG;

/**
 * Holds {@link Timestamp} records by {@code StringValue} IDs.
 */
public final class TimestampByMessage extends MessageTable<StringValue, Timestamp> {

    private static final String NAME = "timestamp_by_message";

    public TimestampByMessage(DataSourceWrapper dataSource, TypeMapping typeMapping) {
        super(NAME, IdColumn.of(Column.ID, StringValue.class), dataSource, typeMapping);
    }

    /**
     * Reads a given ID back from the database as a {@link ResultSet}.
     *
     * <p>Allows to obtain a {@link ResultSet} with a message ID which is sometimes necessary in
     * tests.
     */
    public ResultSet resultSetWithId(StringValue id) {
        SelectMessageId.Builder<StringValue, Timestamp> queryBuilder = SelectMessageId
                .<StringValue, Timestamp>newBuilder()
                .setDataSource(dataSource())
                .setTableName(name())
                .setIdColumn(idColumn())
                .setId(id);
        SelectMessageId<StringValue, Timestamp> query = queryBuilder.build();
        ResultSet resultSet = query.query()
                                   .getResults();
        return resultSet;
    }

    @Override
    public StringValue idOf(Timestamp record) {
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
        ID(Column::idOf),
        SECONDS(LONG, Timestamp::getSeconds),
        NANOS(INT, Timestamp::getNanos);

        private final @Nullable Type type;
        private final Getter<Timestamp> getter;

        Column(@Nullable Type type, Getter<Timestamp> getter) {
            this.type = type;
            this.getter = getter;
        }

        Column(Getter<Timestamp> getter) {
            this(null, getter);
        }

        @Override
        public Getter<Timestamp> getter() {
            return getter;
        }

        @Override
        public @Nullable Type type() {
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

        private static StringValue idOf(Timestamp timestamp) {
            String value = Timestamps.toString(timestamp);
            StringValue id = StringValue
                    .newBuilder()
                    .setValue(value)
                    .build();
            return id;
        }
    }
}
