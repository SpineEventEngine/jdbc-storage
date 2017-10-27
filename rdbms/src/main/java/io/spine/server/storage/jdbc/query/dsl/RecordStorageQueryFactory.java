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

package io.spine.server.storage.jdbc.query.dsl;

import com.google.protobuf.FieldMask;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.EntityQuery;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.DataSourceWrapper;
import io.spine.server.storage.jdbc.IdColumn;
import io.spine.server.storage.jdbc.RecordTable;
import io.spine.server.storage.jdbc.query.ReadQueryFactory;
import io.spine.server.storage.jdbc.query.SelectByEntityColumnsQuery;
import io.spine.server.storage.jdbc.query.SelectByIdQuery;
import io.spine.server.storage.jdbc.query.SelectQuery;
import io.spine.server.storage.jdbc.query.WriteQuery;
import io.spine.server.storage.jdbc.query.WriteQueryFactory;
import io.spine.server.storage.jdbc.type.JdbcColumnType;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The query factory for interaction with the {@link RecordTable}.
 *
 * @author Andrey Lavrov
 * @author Dmytro Dashenkov
 */
public class RecordStorageQueryFactory<I>
        implements ReadQueryFactory<I, EntityRecord>,
                   WriteQueryFactory<I, EntityRecordWithColumns> {

    private final IdColumn<I> idColumn;
    private final DataSourceWrapper dataSource;
    private final String tableName;
    private final Logger logger;
    private final ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>> columnTypeRegistry;

    /**
     * Creates a new instance.
     *
     * @param dataSource instance of {@link DataSourceWrapper}
     * @param tableName  the name of the table to generate queries for
     */
    public RecordStorageQueryFactory(DataSourceWrapper dataSource,
                                     String tableName,
                                     Logger logger,
                                     IdColumn<I> idColumn,
                                     ColumnTypeRegistry<? extends JdbcColumnType<? super Object, ? super Object>>
                                             columnTypeRegistry) {
        super();
        this.idColumn = checkNotNull(idColumn);
        this.dataSource = dataSource;
        this.tableName = tableName;
        this.logger = logger;
        this.columnTypeRegistry = columnTypeRegistry;
    }

    public Logger getLogger() {
        return logger;
    }

    public SelectByEntityColumnsQuery<I> newSelectByEntityQuery(EntityQuery<I> query, FieldMask fieldMask) {
        final SelectByEntityColumnsQuery.Builder<I> builder =
                SelectByEntityColumnsQuery.<I>newBuilder()
                                          .setDataSource(dataSource)
                                          .setLogger(logger)
                                          .setTableName(tableName)
                                          .setIdColumn(idColumn)
                                          .setColumnTypeRegistry(columnTypeRegistry)
                                          .setEntityQuery(query)
                                          .setFieldMask(fieldMask);
        return builder.build();
    }

    public WriteQuery newInsertEntityRecordsBulkQuery(Map<I, EntityRecordWithColumns> records) {
        final InsertEntityRecordsBulkQuery.Builder<I> builder = InsertEntityRecordsBulkQuery.newBuilder();
        builder.setDataSource(dataSource)
               .setTableName(tableName)
               .setIdColumn(idColumn)
               .setColumnTypeRegistry(columnTypeRegistry)
               .addRecords(records);
        return builder.build();
    }

    @Override
    public SelectByIdQuery<I, EntityRecord> newSelectByIdQuery(I id) {
        final SelectEntityByIdQuery.Builder<I> builder = SelectEntityByIdQuery.newBuilder();
        builder.setTableName(tableName)
               .setDataSource(dataSource)
               .setIdColumn(idColumn)
               .setId(id);
        return builder.build();
    }

    @Override
    public SelectQuery<Iterator<I>> newIndexQuery() {
        final StorageIndexQuery.Builder<I> builder = StorageIndexQuery.newBuilder();
        return builder.setDataSource(dataSource)
                      .setTableName(tableName)
                      .setIdColumn(idColumn)
                      .build();
    }

    @Override
    public WriteQuery newInsertQuery(I id, EntityRecordWithColumns record) {
        final InsertEntityQuery.Builder<I> builder = InsertEntityQuery.newBuilder();
        builder.setDataSource(dataSource)
               .setTableName(tableName)
               .setId(id)
               .setIdColumn(idColumn)
               .setColumnTypeRegistry(columnTypeRegistry)
               .setRecord(record);
        return builder.build();
    }

    @Override
    public WriteQuery newUpdateQuery(I id, EntityRecordWithColumns record) {
        final UpdateEntityQuery.Builder<I> builder = UpdateEntityQuery.newBuilder();
        builder.setTableName(tableName)
               .setDataSource(dataSource)
               .setIdColumn(idColumn)
               .setId(id)
               .setRecord(record)
               .setColumnTypeRegistry(columnTypeRegistry);
        return builder.build();
    }
}
