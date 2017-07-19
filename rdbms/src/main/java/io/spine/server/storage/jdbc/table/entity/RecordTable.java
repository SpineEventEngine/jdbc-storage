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

package io.spine.server.storage.jdbc.table.entity;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.protobuf.FieldMask;
import io.spine.server.entity.Entity;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.Column;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.EntityColumns;
import io.spine.server.entity.storage.EntityQuery;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.Sql;
import io.spine.server.storage.jdbc.entity.JdbcRecordStorage;
import io.spine.server.storage.jdbc.entity.query.RecordStorageQueryFactory;
import io.spine.server.storage.jdbc.entity.query.SelectBulkQuery;
import io.spine.server.storage.jdbc.entity.query.SelectByEntityQuery;
import io.spine.server.storage.jdbc.query.ReadQueryFactory;
import io.spine.server.storage.jdbc.query.WriteQueryFactory;
import io.spine.server.storage.jdbc.table.TableColumn;
import io.spine.server.storage.jdbc.type.JdbcColumnType;
import io.spine.server.storage.jdbc.util.DataSourceWrapper;

import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Lists.newLinkedList;
import static io.spine.server.storage.jdbc.Sql.Type.BLOB;
import static io.spine.server.storage.jdbc.Sql.Type.ID;
import static java.util.Collections.addAll;

/**
 * A table for storing the {@linkplain EntityRecord entity records}.
 *
 * <p>Used in the {@link JdbcRecordStorage}.
 *
 * @author Dmytro Dashenkov
 */
public class RecordTable<I> extends EntityTable<I, EntityRecord, EntityRecordWithColumns> {

    private final RecordStorageQueryFactory<I> queryFactory;

    private final ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> typeRegistry;

    public RecordTable(Class<Entity<I, ?>> entityClass,
                       DataSourceWrapper dataSource,
                       ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> columnTypeRegistry) {
        super(entityClass, StandardColumn.id.name(), dataSource);
        queryFactory = new RecordStorageQueryFactory<>(dataSource,
                                                       entityClass,
                                                       log(),
                                                       getIdColumn(),
                                                       columnTypeRegistry);
        this.typeRegistry = columnTypeRegistry;
    }

    @Override
    public StandardColumn getIdColumnDeclaration() {
        return StandardColumn.id;
    }

    @Override
    protected ReadQueryFactory<I, EntityRecord> getReadQueryFactory() {
        return queryFactory;
    }

    @Override
    protected WriteQueryFactory<I, EntityRecordWithColumns> getWriteQueryFactory() {
        return queryFactory;
    }

    @Override
    protected List<TableColumn> getTableColumns() {
        final List<TableColumn> columns = newLinkedList();
        addAll(columns, StandardColumn.values());
        final Collection<Column> entityColumns = EntityColumns.getColumns(getEntityClass());
        final Collection<TableColumn> tableColumns = transform(entityColumns, new ColumnAdapter());
        columns.addAll(tableColumns);
        return columns;
    }

    public Map<?, EntityRecord> read(Iterable<I> ids, FieldMask fieldMask) {
        try {
            final Map<?, EntityRecord> recordMap = queryFactory.newSelectBulkQuery(ids, fieldMask)
                                                               .execute();
            return recordMap;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public void write(Map<I, EntityRecordWithColumns> records) {
        // Map's initial capacity is maximum, meaning no records exist in the storage yet

        final Map<I, EntityRecordWithColumns> newRecords = new HashMap<>(records.size());

        for (Map.Entry<I, EntityRecordWithColumns> unclassifiedRecord : records.entrySet()) {
            final I id = unclassifiedRecord.getKey();
            final EntityRecordWithColumns record = unclassifiedRecord.getValue();
            if (containsRecord(id)) {
                queryFactory.newUpdateQuery(id, unclassifiedRecord.getValue())
                            .execute();
            } else {
                newRecords.put(id, record);
            }
        }
        if (!newRecords.isEmpty()) {
            queryFactory.newInsertEntityRecordsBulkQuery(newRecords).execute();
        }
    }

    public Map<I, EntityRecord> readAll(FieldMask fieldMask) {
        final SelectBulkQuery<I> query = queryFactory.newSelectAllQuery(fieldMask);
        try {
            final Map<I, EntityRecord> result = query.execute();
            return result;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public Map<I, EntityRecord> readByQuery(EntityQuery<I> query, FieldMask fieldMask) {
        final SelectByEntityQuery<I> queryByEntity =
                queryFactory.newSelectByEntityQuery(query, fieldMask);

        try {
            final Map<I, EntityRecord> result = queryByEntity.execute();
            return result;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    private final class ColumnAdapter implements Function<Column, TableColumn> {

        private int columnNumber = 1;

        @Override
        public TableColumn apply(@Nullable Column column) {
            checkNotNull(column);
            final TableColumn result = new EntityColumnMapping(column, typeRegistry, columnNumber);
            columnNumber++;
            return result;
        }
    }

    public enum StandardColumn implements TableColumn {

        id(ID),
        entity(BLOB);

        private final Sql.Type type;

        StandardColumn(Sql.Type type) {
            this.type = type;
        }

        @Override
        public Sql.Type type() {
            return type;
        }
    }

    private static final class EntityColumnMapping implements TableColumn {

        private final String name;
        private final int ordinal;
        private final Sql.Type type;

        private EntityColumnMapping(Column column,
                                    ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> typeRegistry,
                                    int ordinal) {
            this.name = column.getName();
            this.ordinal = ordinal;
            this.type = typeRegistry.get(column)
                                    .getSqlType();
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public int ordinal() {
            return ordinal;
        }

        @Override
        public Sql.Type type() {
            return type;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            EntityColumnMapping that = (EntityColumnMapping) o;
            return ordinal == that.ordinal &&
                   Objects.equal(name, that.name) &&
                   type == that.type;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(name, ordinal, type);
        }
    }
}
