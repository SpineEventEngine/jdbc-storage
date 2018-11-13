/*
 * Copyright 2018, TeamDev. All rights reserved.
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
import io.spine.server.entity.Entity;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.EntityColumn;
import io.spine.server.entity.storage.EntityQuery;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.TableColumn;
import io.spine.server.storage.jdbc.Type;
import io.spine.server.storage.jdbc.TypeMapping;
import io.spine.server.storage.jdbc.query.EntityTable;
import io.spine.server.storage.jdbc.query.PairedValue;
import io.spine.server.storage.jdbc.query.SelectQuery;
import io.spine.server.storage.jdbc.query.WriteQuery;
import io.spine.server.storage.jdbc.type.JdbcColumnType;
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
import static java.util.Collections.addAll;

/**
 * A table for storing the {@linkplain EntityRecord entity records}.
 *
 * <p>Used in the {@link JdbcRecordStorage}.
 *
 * @author Dmytro Dashenkov
 */
class RecordTable<I> extends EntityTable<I, EntityRecord, EntityRecordWithColumns> {

    private final ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>> typeRegistry;
    private final Collection<EntityColumn> entityColumns;

    RecordTable(Class<? extends Entity<I, ?>> entityClass,
                DataSourceWrapper dataSource,
                ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>>
                        columnTypeRegistry,
                TypeMapping typeMapping,
                Collection<EntityColumn> entityColumns) {
        super(entityClass, StandardColumn.ID.name(), dataSource, typeMapping);
        this.typeRegistry = columnTypeRegistry;
        this.entityColumns = entityColumns;
    }

    @Override
    protected StandardColumn getIdColumnDeclaration() {
        return StandardColumn.ID;
    }

    @Override
    protected List<TableColumn> getTableColumns() {
        List<TableColumn> columns = newLinkedList();
        addAll(columns, StandardColumn.values());
        Collection<TableColumn> tableColumns = entityColumns.stream()
                                                            .map(new ColumnAdapter())
                                                            .collect(Collectors.toList());
        columns.addAll(tableColumns);
        return columns;
    }

    @Override
    protected SelectQuery<EntityRecord> composeSelectQuery(I id) {
        SelectEntityByIdQuery.Builder<I> builder = SelectEntityByIdQuery.newBuilder();
        SelectEntityByIdQuery<I> query = builder.setTableName(getName())
                                                .setDataSource(getDataSource())
                                                .setIdColumn(getIdColumn())
                                                .setId(id)
                                                .build();
        return query;
    }

    @Override
    protected WriteQuery composeInsertQuery(I id, EntityRecordWithColumns record) {
        InsertEntityQuery.Builder<I> builder = InsertEntityQuery.newBuilder();
        InsertEntityQuery query = builder.setDataSource(getDataSource())
                                         .setTableName(getName())
                                         .setIdColumn(getIdColumn())
                                         .setColumnTypeRegistry(typeRegistry)
                                         .addRecord(id, record)
                                         .build();
        return query;
    }

    @Override
    protected WriteQuery composeUpdateQuery(I id, EntityRecordWithColumns record) {
        UpdateEntityQuery.Builder<I> builder = UpdateEntityQuery.newBuilder();
        UpdateEntityQuery query = builder.setTableName(getName())
                                         .setDataSource(getDataSource())
                                         .setIdColumn(getIdColumn())
                                         .addRecord(id, record)
                                         .setColumnTypeRegistry(typeRegistry)
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
     * number of IDs in {@code EntityQuery}.
     *
     * <p>The resulting record fields are masked by the given {@code FieldMask}.
     */
    Iterator<EntityRecord> readByIds(EntityQuery<I> entityQuery, FieldMask fieldMask) {
        Iterator<PairedValue<I, EntityRecord>> response = executeQuery(entityQuery, fieldMask);
        Iterator<EntityRecord> records = extractRecordsForIds(entityQuery.getIds(), response);
        return records;
    }

    /**
     * Executes an {@code EntityQuery} and returns all records that match.
     *
     * <p>The resulting record fields are masked by the given {@code FieldMask}.
     *
     * <p>This method is more effective performance-wise than its
     * {@link #readByIds(EntityQuery, FieldMask)} counterpart.
     */
    Iterator<EntityRecord> readByQuery(EntityQuery<I> entityQuery, FieldMask fieldMask) {
        Iterator<PairedValue<I, EntityRecord>> response = executeQuery(entityQuery, fieldMask);
        Iterator<EntityRecord> records = extractRecords(response);
        return records;
    }

    private Iterator<PairedValue<I, EntityRecord>>
    executeQuery(EntityQuery<I> entityQuery, FieldMask fieldMask) {
        SelectByEntityColumnsQuery.Builder<I> builder = SelectByEntityColumnsQuery.newBuilder();
        SelectByEntityColumnsQuery<I> query = builder.setDataSource(getDataSource())
                                                     .setTableName(getName())
                                                     .setIdColumn(getIdColumn())
                                                     .setColumnTypeRegistry(typeRegistry)
                                                     .setEntityQuery(entityQuery)
                                                     .setFieldMask(fieldMask)
                                                     .build();
        Iterator<PairedValue<I, EntityRecord>> queryResult = query.execute();
        return queryResult;
    }

    private Iterator<EntityRecord>
    extractRecordsForIds(Iterable<I> ids, Iterator<PairedValue<I, EntityRecord>> queryResponse) {
        Map<I, EntityRecord> presentRecords = new HashMap<>();
        queryResponse.forEachRemaining(
                record -> presentRecords.put(record.aValue(), record.bValue())
        );
        Iterator<EntityRecord> result = stream(ids)
                .map(presentRecords::get)
                .iterator();
        return result;
    }

    private Iterator<EntityRecord>
    extractRecords(Iterator<PairedValue<I, EntityRecord>> queryResponse) {
        Iterator<EntityRecord> result = stream(queryResponse)
                .map(PairedValue::bValue)
                .iterator();
        return result;
    }

    private WriteQuery newInsertEntityRecordsBulkQuery(Map<I, EntityRecordWithColumns> records) {
        InsertEntityQuery.Builder<I> builder = InsertEntityQuery.newBuilder();
        InsertEntityQuery query = builder.setDataSource(getDataSource())
                                         .setTableName(getName())
                                         .setIdColumn(getIdColumn())
                                         .setColumnTypeRegistry(typeRegistry)
                                         .addRecords(records)
                                         .build();
        return query;
    }

    /**
     * The enumeration of the {@link RecordTable} standard columns common to all the Database tables
     * represented by the {@code RecordTable}.
     *
     * <p>Each table which contains the {@linkplain EntityRecord Entity Records} has these columns.
     * It also may have the columns produced from the {@linkplain EntityColumn entity columns}.
     *
     * @see EntityColumnWrapper
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
     * An adapter converting the {@linkplain EntityColumn entity columns}
     * into the {@link TableColumn} instances.
     */
    private final class ColumnAdapter implements Function<EntityColumn, TableColumn> {

        @Override
        public TableColumn apply(@Nullable EntityColumn column) {
            checkNotNull(column);
            TableColumn result = new EntityColumnWrapper(column, typeRegistry);
            return result;
        }
    }

    /**
     * A wrapper type for {@link EntityColumn}.
     *
     * <p>Serves for accessing entity columns trough the {@link TableColumn} interface.
     *
     * @see StandardColumn
     */
    private static final class EntityColumnWrapper implements TableColumn {

        private final EntityColumn column;
        private final Type type;

        private EntityColumnWrapper(EntityColumn column,
                                    ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> typeRegistry) {
            this.column = column;
            this.type = typeRegistry.get(column)
                                    .getType();
        }

        @Override
        public String name() {
            return column.getStoredName();
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
            EntityColumnWrapper that = (EntityColumnWrapper) o;
            return Objects.equal(column, that.column) &&
                   type == that.type;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(column, type);
        }
    }
}
