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

import com.google.protobuf.FieldMask;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.DatabaseException;
import io.spine.server.storage.jdbc.entity.JdbcRecordStorage;
import io.spine.server.storage.jdbc.entity.query.RecordStorageQueryFactory;
import io.spine.server.storage.jdbc.entity.query.SelectBulkQuery;
import io.spine.server.storage.jdbc.query.ReadQueryFactory;
import io.spine.server.storage.jdbc.query.WriteQueryFactory;
import io.spine.server.storage.jdbc.type.JdbcColumnType;
import io.spine.server.storage.jdbc.util.DataSourceWrapper;
import io.spine.server.entity.Entity;
import io.spine.server.entity.EntityRecord;
import io.spine.server.storage.jdbc.Sql;
import io.spine.server.storage.jdbc.table.TableColumn;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static io.spine.server.storage.jdbc.Sql.Type.BLOB;
import static io.spine.server.storage.jdbc.Sql.Type.BOOLEAN;
import static io.spine.server.storage.jdbc.Sql.Type.ID;

/**
 * A table for storing the {@linkplain EntityRecord entity records}.
 *
 * <p>Used in the {@link JdbcRecordStorage}.
 *
 * @author Dmytro Dashenkov
 */
public class RecordTable<I> extends EntityTable<I, EntityRecord, RecordTable.Column> {

    private final RecordStorageQueryFactory<I> queryFactory;


    public RecordTable(Class<Entity<I, ?>> entityClass,
                       DataSourceWrapper dataSource,
                       ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> columnTypeRegistry) {
        super(entityClass, Column.id.name(), dataSource, columnTypeRegistry);
        queryFactory = new RecordStorageQueryFactory<>(dataSource,
                                                       entityClass,
                                                       log(),
                                                       getIdColumn(),
                                                       columnTypeRegistry);
    }

    @Override
    public Column getIdColumnDeclaration() {
        return Column.id;
    }

    @Override
    protected Class<Column> getTableColumnType() {
        return Column.class;
    }

    @Override
    protected ReadQueryFactory<I, EntityRecord> getReadQueryFactory() {
        return queryFactory;
    }

    @Override
    protected WriteQueryFactory<I, EntityRecord> getWriteQueryFactory() {
        return null;
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

    public void write(I id, EntityRecordWithColumns record) {
        final Map<String, io.spine.server.entity.storage.Column> columns = record.getColumns();
//        createIfNotExists(columns);

        if (containsRecord(id)) {
            queryFactory.newUpdateQuery(id, record).execute();
        } else {
            queryFactory.newInsertQuery(id, record).execute();
        }
    }

    public void write(Map<I, EntityRecordWithColumns> records) {
        // Map's initial capacity is maximum, meaning no records exist in the storage yet

        final Map<String, io.spine.server.entity.storage.Column> columns =
                records.values().iterator().next().getColumns();
//        createIfNotExists(columns);

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
        queryFactory.newInsertEntityRecordsBulkQuery(newRecords)
                    .execute();
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

    public enum Column implements TableColumn, Cloneable {

        id(ID),
        entity(BLOB),
        archived(BOOLEAN),
        deleted(BOOLEAN);

        private final Sql.Type type;

        Column(Sql.Type type) {
            this.type = type;
        }

        @Override
        public Sql.Type type() {
            return type;
        }
    }
}
