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

package io.spine.server.storage.jdbc.query;

import com.google.protobuf.FieldMask;
import io.spine.annotation.Internal;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.EntityQuery;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.IdColumn;
import io.spine.server.storage.jdbc.type.JdbcColumnType;

import java.util.Iterator;
import java.util.Map;

/**
 * An implementation of the query factory for generating read queries for
 * the {@link io.spine.server.storage.jdbc.RecordTable RecordTable}.
 *
 * @author Andrey Lavrov
 * @author Dmytro Dashenkov
 */
@Internal
public class RecordStorageReadQueryFactory<I> extends AbstractReadQueryFactory<I, EntityRecord> {

    private final ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>> columnTypeRegistry;

    /**
     * Creates a new instance.
     *
     * @param dataSource instance of {@link DataSourceWrapper}
     * @param tableName  the name of the table to generate queries for
     */
    public RecordStorageReadQueryFactory(DataSourceWrapper dataSource,
                                         String tableName,
                                         IdColumn<I> idColumn,
                                         ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>>
                                             columnTypeRegistry) {
        super(idColumn, dataSource, tableName);
        this.columnTypeRegistry = columnTypeRegistry;
    }

    public SelectQuery<Iterator<EntityRecord>> newSelectByEntityQuery(EntityQuery<I> query,
                                                                      FieldMask fieldMask) {
        final SelectByEntityColumnsQuery.Builder<I> builder = SelectByEntityColumnsQuery.newBuilder();
        builder.setDataSource(getDataSource())
               .setTableName(getTableName())
               .setIdColumn(getIdColumn())
               .setColumnTypeRegistry(columnTypeRegistry)
               .setEntityQuery(query)
               .setFieldMask(fieldMask);
        return builder.build();
    }

    public WriteQuery newInsertEntityRecordsBulkQuery(Map<I, EntityRecordWithColumns> records) {
        final InsertEntityRecordsBulkQuery.Builder<I> builder = InsertEntityRecordsBulkQuery.newBuilder();
        builder.setDataSource(getDataSource())
               .setTableName(getTableName())
               .setIdColumn(getIdColumn())
               .setColumnTypeRegistry(columnTypeRegistry)
               .addRecords(records);
        return builder.build();
    }

    @Override
    public SelectByIdQuery<I, EntityRecord> newSelectByIdQuery(I id) {
        final SelectEntityByIdQuery.Builder<I> builder = SelectEntityByIdQuery.newBuilder();
        builder.setTableName(getTableName())
               .setDataSource(getDataSource())
               .setIdColumn(getIdColumn())
               .setId(id);
        return builder.build();
    }
}
