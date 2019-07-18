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

package io.spine.server.storage.jdbc.message;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import io.spine.protobuf.AnyPacker;
import io.spine.reflect.GenericTypeIndex;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.query.AbstractTable;
import io.spine.server.storage.jdbc.query.IdColumn;
import io.spine.type.TypeUrl;

import java.util.List;
import java.util.function.Function;

import static io.spine.server.storage.jdbc.Type.BYTE_ARRAY;

/**
 * A table storing a single {@link Message} type by ID.
 *
 * @param <I> the {@code Message} ID type
 * @param <M> the {@code Message} type
 */
public abstract class MessageTable<I, M extends Message> extends AbstractTable<I, M, M> {

    private final TypeUrl typeUrl;

    protected MessageTable(String name,
                           IdColumn<I> idColumn,
                           DataSourceWrapper dataSource,
                           TypeMapping typeMapping) {
        super(name, idColumn, dataSource, typeMapping);
        @SuppressWarnings("unchecked")      // Ensured by the class declaration.
                Class<M> messageClass = (Class<M>) GenericParameter.MESSAGE.argumentIn(getClass());
        this.typeUrl = TypeUrl.of(messageClass);
    }

    public void write(M record) {
    }

    @Override
    protected InsertMessageQuery<I, M> composeInsertQuery(I id, M record) {
        InsertMessageQuery.Builder<I, M> builder = InsertMessageQuery.newBuilder();
        InsertMessageQuery<I, M> query = builder.setTableName(name())
                                                .setDataSource(dataSource())
                                                .setIdColumn(idColumn())
                                                .setId(id)
                                                .setMessage(record)
                                                .setColumns(columns())
                                                .build();
        return query;
    }

    @Override
    protected UpdateMessageQuery<I, M> composeUpdateQuery(I id, M record) {
        UpdateMessageQuery.Builder<I, M> builder = UpdateMessageQuery.newBuilder();
        UpdateMessageQuery<I, M> query = builder.setTableName(name())
                                                .setDataSource(dataSource())
                                                .setIdColumn(idColumn())
                                                .setId(id)
                                                .setMessage(record)
                                                .setColumns(columns())
                                                .build();
        return query;
    }

    @Override
    protected SelectMessageById<I, M> composeSelectQuery(I id) {
        SelectMessageById.Builder<I, M> builder = SelectMessageById.newBuilder();
        SelectMessageById<I, M> query = builder.setTableName(name())
                                               .setDataSource(dataSource())
                                               .setIdColumn(idColumn())
                                               .setId(id)
                                               .setBytesColumn(DefaultColumn.bytes())
                                               .setTypeUrl(typeUrl)
                                               .doBuild();
        return query;
    }

    @Override
    protected List<? extends Column<M>> tableColumns() {
        ImmutableList.Builder<Column<M>> columns = ImmutableList.builder();
        columns.addAll(columns());
        columns.add(DefaultColumn.bytes());
        return columns.build();
    }

    protected abstract ImmutableList<? extends Column<M>> columns();

    public interface Column<M extends Message> extends TableColumn {

        Getter<M> getter();

        @FunctionalInterface
        interface Getter<M extends Message> extends Function<M, Object> {
        }
    }

    private static class DefaultColumn<M extends Message> implements Column<M> {

        private final String name;
        private final Type type;
        private final Getter<M> getter;

        private DefaultColumn(String name, Type type, Getter<M> getter) {
            this.name = name;
            this.type = type;
            this.getter = getter;
        }

        private static <M extends Message> DefaultColumn<M> bytes() {
            return new DefaultColumn<>("bytes", BYTE_ARRAY, AnyPacker::pack);
        }

        @Override
        public Getter<M> getter() {
            return getter;
        }

        @Override
        public String name() {
            return name;
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

    /**
     * Enumeration of generic type parameters of this abstract class.
     */
    private enum GenericParameter implements GenericTypeIndex<MessageTable> {

        /**
         * The index of the declaration of the generic parameter type {@code <I>} in
         * the {@link MessageTable} abstract class.
         */
        ID(0),

        /**
         * The index of the declaration of the generic parameter type {@code <M>}
         * in the {@link MessageTable} abstract class.
         */
        MESSAGE(1);

        private final int index;

        GenericParameter(int index) {
            this.index = index;
        }

        @Override
        public int index() {
            return index;
        }
    }
}
