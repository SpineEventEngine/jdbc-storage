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

package io.spine.server.storage.jdbc;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.protobuf.FieldMask;
import io.spine.annotation.Internal;
import io.spine.server.entity.Entity;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.EntityColumn;
import io.spine.server.entity.storage.EntityColumns;
import io.spine.server.entity.storage.EntityQuery;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.query.ReadQueryFactory;
import io.spine.server.storage.jdbc.query.WriteQueryFactory;
import io.spine.server.storage.jdbc.query.RecordStorageReadQueryFactory;
import io.spine.server.storage.jdbc.query.RecordStorageWriteQueryFactory;
import io.spine.server.storage.jdbc.type.JdbcColumnType;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Lists.newLinkedList;
import static io.spine.server.storage.jdbc.DbTableNameFactory.newTableName;
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
@Internal
public class RecordTable<I> extends EntityTable<I, EntityRecord, EntityRecordWithColumns> {

    private final RecordStorageWriteQueryFactory<I> writeQueryFactory;
    private final RecordStorageReadQueryFactory<I> readQueryFactory;

    private final ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>> typeRegistry;

    RecordTable(Class<? extends Entity<I, ?>> entityClass,
                DataSourceWrapper dataSource,
                ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>>
                        columnTypeRegistry) {
        super(entityClass, StandardColumn.id.name(), dataSource);
        final String tableName = newTableName(entityClass);
        this.typeRegistry = columnTypeRegistry;
        readQueryFactory = new RecordStorageReadQueryFactory<>(dataSource,
                                                               tableName,
                                                               getIdColumn(),
                                                               columnTypeRegistry);
        writeQueryFactory = new RecordStorageWriteQueryFactory<>(getIdColumn(),
                                                                 dataSource,
                                                                 tableName,
                                                                 columnTypeRegistry);
    }

    @Override
    protected StandardColumn getIdColumnDeclaration() {
        return StandardColumn.id;
    }

    @Override
    protected ReadQueryFactory<I, EntityRecord> getReadQueryFactory() {
        return readQueryFactory;
    }

    @Override
    protected WriteQueryFactory<I, EntityRecordWithColumns> getWriteQueryFactory() {
        return writeQueryFactory;
    }

    @Override
    protected List<TableColumn> getTableColumns() {
        final List<TableColumn> columns = newLinkedList();
        addAll(columns, StandardColumn.values());
        final Collection<EntityColumn> entityColumns = EntityColumns.getColumns(getEntityClass());
        final Collection<TableColumn> tableColumns = transform(entityColumns, new ColumnAdapter());
        columns.addAll(tableColumns);
        return columns;
    }

    void write(Map<I, EntityRecordWithColumns> records) {
        // Map's initial capacity is maximum, meaning no records exist in the storage yet

        final Map<I, EntityRecordWithColumns> newRecords = new HashMap<>(records.size());

        for (Map.Entry<I, EntityRecordWithColumns> unclassifiedRecord : records.entrySet()) {
            final I id = unclassifiedRecord.getKey();
            final EntityRecordWithColumns record = unclassifiedRecord.getValue();
            if (containsRecord(id)) {
                writeQueryFactory.newUpdateQuery(id, unclassifiedRecord.getValue())
                                 .execute();
            } else {
                newRecords.put(id, record);
            }
        }
        if (!newRecords.isEmpty()) {
            readQueryFactory.newInsertEntityRecordsBulkQuery(newRecords)
                            .execute();
        }
    }

    Iterator<EntityRecord> readByQuery(EntityQuery<I> query, FieldMask fieldMask) {
        final Iterator<EntityRecord> result = readQueryFactory.newSelectByEntityQuery(query, fieldMask)
                                                              .execute();
        return result;
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
    @Internal
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

        @Override
        public boolean isPrimaryKey() {
            return this == id;
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
            final TableColumn result = new EntityColumnWrapper(column, typeRegistry);
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
        private final Sql.Type type;

        private EntityColumnWrapper(EntityColumn column,
                                    ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> typeRegistry) {
            this.column = column;
            this.type = typeRegistry.get(column)
                                    .getSqlType();
        }

        @Override
        public String name() {
            return column.getStoredName();
        }

        @Override
        public Sql.Type type() {
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
