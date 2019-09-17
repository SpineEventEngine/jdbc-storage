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
import com.google.errorprone.annotations.Immutable;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.query.AbstractTable;
import io.spine.server.storage.jdbc.query.IdColumn;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Functions.identity;
import static com.google.common.collect.Streams.stream;
import static io.spine.server.storage.jdbc.Type.BYTE_ARRAY;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * An SQL table storing a single {@link Message} type.
 *
 * @param <I>
 *         the record ID type
 * @param <M>
 *         the message type
 */
public abstract class MessageTable<I, M extends Message> extends AbstractTable<I, M, M> {

    protected MessageTable(String name,
                           IdColumn<I> idColumn,
                           DataSourceWrapper dataSource,
                           TypeMapping typeMapping) {
        super(name, idColumn, dataSource, typeMapping);
    }

    /**
     * Obtains multiple messages from the table by IDs.
     *
     * <p>The non-existent IDs are ignored.
     */
    Iterator<M> readAll(Iterable<I> ids) {
        SelectMessagesInBulk<I, M> query = composeSelectMessagesInBulkQuery(ids);
        Iterator<M> result = query.execute();
        return result;
    }

    /**
     * Writes a single message to the storage.
     *
     * <p>If a record with the same ID already exists, it is overwritten.
     */
    void write(M record) {
        write(idOf(record), record);
    }

    /**
     * Writes multiple records to the storage in a batch.
     *
     * <p>If some of the records have IDs that already exist, the respective records in the DB are
     * overwritten.
     */
    void writeAll(Iterable<M> records) {
        Collection<I> existingIds = existingIds(records);

        Map<I, M> existingRecords = stream(records)
                .filter(record -> existingIds.contains(idOf(record)))
                .collect(toMap(this::idOf, identity()));
        Map<I, M> newRecords = stream(records)
                .filter(record -> !existingIds.contains(idOf(record)))
                .collect(toMap(this::idOf, identity()));

        updateAll(existingRecords);
        insertAll(newRecords);
    }

    /**
     * Removes multiple records from the table in a bulk.
     *
     * <p>Non-existent records are ignored.
     */
    void removeAll(Iterable<M> records) {
        List<I> ids = stream(records)
                .map(this::idOf)
                .collect(toList());
        DeleteMessagesInBulk<I> query = composeDeleteMessagesInBulkQuery(ids);
        query.execute();
    }

    private Collection<I> existingIds(Iterable<M> records) {
        Iterable<I> ids = stream(records)
                .map(this::idOf)
                .collect(toList());

        List<I> existingIds = stream(readAll(ids))
                .map(this::idOf)
                .collect(toList());
        return existingIds;
    }

    private void insertAll(Map<I, M> records) {
        InsertMessagesInBulk<I, M> query = composeInsertMessagesInBulkQuery(records);
        query.execute();
    }

    private void updateAll(Map<I, M> records) {
        UpdateMessagesInBulk<I, M> query = composeUpdateMessagesInBulkQuery(records);
        query.execute();
    }

    /**
     * Obtains a {@code Descriptor} of the stored message type.
     */
    protected abstract Descriptor messageDescriptor();

    @SuppressWarnings("unchecked") // Ensured by descendant classes declaration.
    protected I idOf(M record) {
        Column<M> column = (Column<M>) idColumn().column();
        I id = (I) column.getter()
                         .apply(record);
        return id;
    }

    @Override
    protected List<? extends Column<M>> tableColumns() {
        ImmutableList.Builder<Column<M>> columns = ImmutableList.builder();
        columns.addAll(messageSpecificColumns());
        columns.addAll(commonColumns());
        return columns.build();
    }

    /**
     * Obtains columns specific to a concrete subtype of the {@code MessageTable}.
     */
    protected abstract Iterable<? extends Column<M>> messageSpecificColumns();

    /**
     * Obtains columns common for all {@code MessageTable} instances.
     */
    private Iterable<? extends Column<M>> commonColumns() {
        return ImmutableList.of(bytesColumn());
    }

    @Override
    protected InsertSingleMessage<I, M> composeInsertQuery(I id, M record) {
        InsertSingleMessage.Builder<I, M> builder = InsertSingleMessage.newBuilder();
        InsertSingleMessage<I, M> query = builder.setTableName(name())
                                                 .setDataSource(dataSource())
                                                 .setIdColumn(idColumn())
                                                 .setId(id)
                                                 .setMessage(record)
                                                 .setColumns(tableColumns())
                                                 .build();
        return query;
    }

    @Override
    protected UpdateSingleMessage<I, M> composeUpdateQuery(I id, M record) {
        UpdateSingleMessage.Builder<I, M> builder = UpdateSingleMessage.newBuilder();
        UpdateSingleMessage<I, M> query = builder.setTableName(name())
                                                 .setDataSource(dataSource())
                                                 .setIdColumn(idColumn())
                                                 .setId(id)
                                                 .setMessage(record)
                                                 .setColumns(tableColumns())
                                                 .build();
        return query;
    }

    @Override
    protected SelectSingleMessage<I, M> composeSelectQuery(I id) {
        SelectSingleMessage.Builder<I, M> builder = SelectSingleMessage.newBuilder();
        SelectSingleMessage<I, M> query = builder.setTableName(name())
                                                 .setDataSource(dataSource())
                                                 .setIdColumn(idColumn())
                                                 .setId(id)
                                                 .setMessageDescriptor(messageDescriptor())
                                                 .doBuild();
        return query;
    }

    private SelectMessagesInBulk<I, M> composeSelectMessagesInBulkQuery(Iterable<I> ids) {
        SelectMessagesInBulk.Builder<I, M> builder = SelectMessagesInBulk.newBuilder();
        SelectMessagesInBulk<I, M> query = builder.setTableName(name())
                                                  .setDataSource(dataSource())
                                                  .setIdColumn(idColumn())
                                                  .setIds(ids)
                                                  .setMessageDescriptor(messageDescriptor())
                                                  .build();
        return query;
    }

    private InsertMessagesInBulk<I, M> composeInsertMessagesInBulkQuery(Map<I, M> records) {
        InsertMessagesInBulk.Builder<I, M> builder = InsertMessagesInBulk.newBuilder();
        InsertMessagesInBulk<I, M> query = builder.setTableName(name())
                                                  .setDataSource(dataSource())
                                                  .setIdColumn(idColumn())
                                                  .setColumns(tableColumns())
                                                  .setRecords(records)
                                                  .build();
        return query;
    }

    private UpdateMessagesInBulk<I, M> composeUpdateMessagesInBulkQuery(Map<I, M> records) {
        UpdateMessagesInBulk.Builder<I, M> builder = UpdateMessagesInBulk.newBuilder();
        UpdateMessagesInBulk<I, M> query = builder.setTableName(name())
                                                  .setDataSource(dataSource())
                                                  .setIdColumn(idColumn())
                                                  .setColumns(tableColumns())
                                                  .setRecords(records)
                                                  .build();
        return query;
    }

    private DeleteMessagesInBulk<I> composeDeleteMessagesInBulkQuery(List<I> ids) {
        DeleteMessagesInBulk.Builder<I> builder = DeleteMessagesInBulk.newBuilder();
        DeleteMessagesInBulk<I> query = builder.setTableName(name())
                                               .setDataSource(dataSource())
                                               .setIdColumn(idColumn())
                                               .setIds(ids)
                                               .build();
        return query;
    }

    /**
     * Obtains a column responsible for storing serialized message bytes.
     */
    public static <M extends Message> BytesColumn<M> bytesColumn() {
        return new BytesColumn<>();
    }

    /**
     * Represents a {@code MessageTable} column.
     *
     * <p>Unlike the ordinary {@link TableColumn}, carries the information on how to extract its
     * data from the given record.
     *
     * @param <M>
     *         the type of messages stored in table
     */
    public interface Column<M extends Message> extends TableColumn {

        Getter<M> getter();

        @Immutable
        @FunctionalInterface
        interface Getter<M extends Message> extends Function<M, Object> {
        }
    }

    /**
     * A column responsible for storing serialized message bytes.
     *
     * <p>This column is present in any {@code MessageTable} and serves for convenient record
     * {@link io.spine.server.storage.jdbc.query.Serializer deserialization}.
     *
     * <p>The column getter can be applied to an arbitrary message and is parameterized only to
     * enable usage along with message-specific table
     * {@linkplain #messageSpecificColumns() columns}.
     *
     * @param <M>
     *         the type of messages stored in the table
     */
    public static class BytesColumn<M extends Message> implements Column<M> {

        private static final String NAME = "bytes";

        @Override
        public Getter<M> getter() {
            return Message::toByteArray;
        }

        @Override
        public String name() {
            return NAME;
        }

        @Override
        public Type type() {
            return BYTE_ARRAY;
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
