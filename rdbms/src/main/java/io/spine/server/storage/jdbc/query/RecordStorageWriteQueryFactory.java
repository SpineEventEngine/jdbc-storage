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

import io.spine.annotation.Internal;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.IdColumn;
import io.spine.server.storage.jdbc.record.RecordTable;
import io.spine.server.storage.jdbc.type.JdbcColumnType;

/**
 * An implementation of the query factory for generating write queries for the {@link RecordTable}.
 *
 * @author Dmytro Grankin
 */
@Internal
public class RecordStorageWriteQueryFactory<I> extends AbstractWriteQueryFactory<I, EntityRecordWithColumns> {

    private final ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>> columnTypeRegistry;

    public RecordStorageWriteQueryFactory(IdColumn<I> idColumn,
                                          DataSourceWrapper dataSource,
                                          String tableName,
                                          ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>>
                                                  columnTypeRegistry) {
        super(idColumn, dataSource, tableName);
        this.columnTypeRegistry = columnTypeRegistry;
    }

    @Override
    public WriteQuery newInsertQuery(I id, EntityRecordWithColumns record) {
        final InsertEntityQuery.Builder<I> builder = InsertEntityQuery.newBuilder();
        builder.setDataSource(getDataSource())
               .setTableName(getTableName())
               .setId(id)
               .setIdColumn(getIdColumn())
               .setColumnTypeRegistry(columnTypeRegistry)
               .setRecord(record);
        return builder.build();
    }

    @Override
    public WriteQuery newUpdateQuery(I id, EntityRecordWithColumns record) {
        final UpdateEntityQuery.Builder<I> builder = UpdateEntityQuery.newBuilder();
        builder.setTableName(getTableName())
               .setDataSource(getDataSource())
               .setIdColumn(getIdColumn())
               .setId(id)
               .setRecord(record)
               .setColumnTypeRegistry(columnTypeRegistry);
        return builder.build();
    }
}
