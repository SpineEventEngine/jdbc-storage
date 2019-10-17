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

package io.spine.server.storage.jdbc.record;

import com.google.common.base.Objects;
import com.google.protobuf.FieldMask;
import io.spine.client.ResponseFormat;
import io.spine.server.entity.Entity;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.Column;
import io.spine.server.entity.storage.Columns;
import io.spine.server.entity.storage.EntityQuery;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.query.DbIterator.DoubleColumnRecord;
import io.spine.server.storage.jdbc.query.EntityTable;
import io.spine.server.storage.jdbc.query.SelectQuery;
import io.spine.server.storage.jdbc.query.WriteQuery;
import io.spine.server.storage.jdbc.type.JdbcTypeRegistry;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Streams.stream;
import static io.spine.server.storage.jdbc.Type.BYTE_ARRAY;
import static io.spine.server.storage.jdbc.record.RecordTable.StandardColumn.ID;
import static java.util.Collections.addAll;

/**
 * A table for storing the {@linkplain EntityRecord entity records}.
 *
 * <p>Used in the {@link JdbcRecordStorage}.
 */
final class RecordTable<I> extends EntityTable<I, EntityRecord, EntityRecordWithColumns> {

    private final JdbcTypeRegistry typeRegistry;
    private final Columns columns;

    RecordTable(Class<? extends Entity<I, ?>> entityClass,
                DataSourceWrapper dataSource,
                JdbcTypeRegistry typeRegistry,
                TypeMapping typeMapping,
                Columns columns) {
        super(entityClass, ID, dataSource, typeMapping);
        this.typeRegistry = typeRegistry;
        this.columns = columns;
    }

    @Override
    protected List<TableColumn> tableColumns() {
        List<TableColumn> columns = newLinkedList();
        addAll(columns, StandardColumn.values());
        Collection<TableColumn> tableColumns = this.columns.columnList()
                                                           .stream()
                                                           .map(new ColumnAdapter())
                                                           .collect(Collectors.toList());
        columns.addAll(tableColumns);
        return columns;
    }

    @Override
    protected SelectQuery<EntityRecord> composeSelectQuery(I id) {
        SelectEntityByIdQuery.Builder<I> builder = SelectEntityByIdQuery.newBuilder();
        SelectEntityByIdQuery<I> query = builder.setTableName(name())
                                                .setDataSource(dataSource())
                                                .setIdColumn(idColumn())
                                                .setId(id)
                                                .build();
        return query;
    }

    @Override
    protected WriteQuery composeInsertQuery(I id, EntityRecordWithColumns record) {
        InsertEntityQuery.Builder<I> builder = InsertEntityQuery.newBuilder();
        InsertEntityQuery query = builder.setDataSource(dataSource())
                                         .setTableName(name())
                                         .setIdColumn(idColumn())
                                         .setTypeRegistry(typeRegistry)
                                         .addRecord(id, record)
                                         .build();
        return query;
    }

    @Override
    protected WriteQuery composeUpdateQuery(I id, EntityRecordWithColumns record) {
        UpdateEntityQuery.Builder<I> builder = UpdateEntityQuery.newBuilder();
        UpdateEntityQuery query = builder.setTableName(name())
                                         .setDataSource(dataSource())
                                         .setIdColumn(idColumn())
                                         .addRecord(id, record)
                                         .setTypeRegistry(typeRegistry)
                                         .build();
        return query;
    }

    void write(Map<I, EntityRecordWithColumns> records) {

        // Map's initial capacity is maximum, meaning no records exist in the storage yet
        Map<I, EntityRecordWithColumns> newRecords = new HashMap<>(records.size());
        for (Map.Entry<I, EntityRecordWithColumns> unclassifiedRecord : records.entrySet()) {
            I id = unclassifiedRecord.getKey();
            EntityRecordWithColumns record = unclassifiedRecord.getValue();
            if (containsRecord(id)) {
                WriteQuery query = composeUpdateQuery(id, unclassifiedRecord.getValue());
                query.execute();
            } else {
                newRecords.put(id, record);
            }
        }
        if (!newRecords.isEmpty()) {
            newInsertEntityRecordsBulkQuery(newRecords).execute();
        }
    }

    /**
     * Executes an {@code EntityQuery} and returns an {@link EntityRecord} or {@code null} for each
     * of the designated IDs.
     *
     * <p>The resulting {@code Iterator} is guaranteed to have the same number of entries as the
     * number of IDs in the {@code EntityQuery}.
     *
     * <p>The resulting record fields are masked by the given {@code FieldMask}.
     */
    Iterator<EntityRecord> readByIds(EntityQuery<I> entityQuery, FieldMask fieldMask) {
        Iterator<DoubleColumnRecord<I, EntityRecord>> response =
                executeQuery(entityQuery, fieldMask);
        Iterator<EntityRecord> records = extractRecordsForIds(entityQuery.getIds(), response);
        return records;
    }

    /**
     * Executes an {@code EntityQuery} and returns all records that match.
     *
     * <p>The resulting record fields are formatted according to the given {@code ResponseFormat}.
     *
     * <p>This method is more effective performance-wise than its
     * {@link #readByIds(EntityQuery, FieldMask)} counterpart.
     */
    Iterator<EntityRecord> readByQuery(EntityQuery<I> entityQuery, ResponseFormat format) {
        Iterator<DoubleColumnRecord<I, EntityRecord>> response =
                executeQuery(entityQuery, format.getFieldMask());
        Iterator<EntityRecord> records = extractRecords(response);
        return records;
    }

    /**
     * Reads all {@linkplain EntityRecord entity records} from the table.
     */
    Iterator<EntityRecord> readAll(ResponseFormat format) {
        Iterator<EntityRecord> result = executeSelectAllQuery(format.getFieldMask());
        return result;
    }

    private Iterator<DoubleColumnRecord<I, EntityRecord>>
    executeQuery(EntityQuery<I> entityQuery, FieldMask fieldMask) {
        SelectByEntityColumnsQuery.Builder<I> builder = SelectByEntityColumnsQuery.newBuilder();
        SelectByEntityColumnsQuery<I> query = builder.setDataSource(dataSource())
                                                     .setTableName(name())
                                                     .setIdColumn(idColumn())
                                                     .typeRegistry(typeRegistry)
                                                     .setEntityQuery(entityQuery)
                                                     .setFieldMask(fieldMask)
                                                     .build();
        Iterator<DoubleColumnRecord<I, EntityRecord>> queryResult = query.execute();
        return queryResult;
    }

    private Iterator<EntityRecord> executeSelectAllQuery(FieldMask fieldMask) {
        SelectAllQuery.Builder builder = SelectAllQuery.newBuilder();
        SelectAllQuery query = builder.setDataSource(dataSource())
                                      .setTableName(name())
                                      .setFieldMask(fieldMask)
                                      .build();
        Iterator<EntityRecord> result = query.execute();
        return result;
    }

    private Iterator<EntityRecord>
    extractRecordsForIds(Iterable<I> ids,
                         Iterator<DoubleColumnRecord<I, EntityRecord>> queryResponse) {
        Map<I, EntityRecord> presentRecords = new HashMap<>();
        queryResponse.forEachRemaining(
                record -> presentRecords.put(record.first(), record.second())
        );
        Iterator<EntityRecord> result = stream(ids)
                .map(presentRecords::get)
                .iterator();
        return result;
    }

    private Iterator<EntityRecord>
    extractRecords(Iterator<DoubleColumnRecord<I, EntityRecord>> queryResponse) {
        Iterator<EntityRecord> result = stream(queryResponse)
                .map(DoubleColumnRecord::second)
                .iterator();
        return result;
    }

    private WriteQuery newInsertEntityRecordsBulkQuery(Map<I, EntityRecordWithColumns> records) {
        InsertEntityQuery.Builder<I> builder = InsertEntityQuery.newBuilder();
        InsertEntityQuery query = builder.setDataSource(dataSource())
                                         .setTableName(name())
                                         .setIdColumn(idColumn())
                                         .setTypeRegistry(typeRegistry)
                                         .addRecords(records)
                                         .build();
        return query;
    }

    /**
     * The enumeration of the {@link RecordTable} standard columns common to all the Database tables
     * represented by the {@code RecordTable}.
     *
     * <p>Each table which contains the {@linkplain EntityRecord Entity Records} has these columns.
     * It also may have the columns produced from the {@linkplain Column entity columns}.
     *
     * @see ColumnWrapper
     */
    enum StandardColumn implements TableColumn {

        ID,
        ENTITY(BYTE_ARRAY);

        private final Type type;

        StandardColumn(Type type) {
            this.type = type;
        }

        /**
         * Creates a column, {@linkplain #type() type} of which is unknown at the compile time.
         */
        StandardColumn() {
            this.type = null;
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

    }

    /**
     * An adapter converting the {@linkplain Column columns}
     * into the {@link TableColumn} instances.
     */
    private final class ColumnAdapter implements Function<Column, TableColumn> {

        @Override
        public TableColumn apply(@Nullable Column column) {
            checkNotNull(column);
            TableColumn result = new ColumnWrapper(column, typeRegistry);
            return result;
        }
    }

    /**
     * A wrapper type for {@link Column}.
     *
     * <p>Serves for accessing entity columns trough the {@link TableColumn} interface.
     *
     * @see StandardColumn
     */
    private static final class ColumnWrapper implements TableColumn {

        private final Column column;
        private final Type type;

        private ColumnWrapper(Column column, JdbcTypeRegistry typeRegistry) {
            this.column = column;
            this.type = typeRegistry.typeOf(column.type());
        }

        @Override
        public String name() {
            return column.name()
                         .value();
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
            return true; // TODO:2017-07-21:dmytro.dashenkov: Use Column.isNullable.
            // https://github.com/SpineEventEngine/jdbc-storage/issues/29
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ColumnWrapper that = (ColumnWrapper) o;
            return Objects.equal(column, that.column) &&
                   type == that.type;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(column, type);
        }
    }
}
