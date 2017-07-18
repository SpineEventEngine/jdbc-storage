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

package io.spine.server.storage.jdbc.entity.query;

import com.google.protobuf.FieldMask;
import io.spine.server.entity.storage.ColumnTypeRegistry;
import io.spine.server.entity.storage.EntityQuery;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.jdbc.query.ReadQueryFactory;
import io.spine.server.storage.jdbc.query.SelectMessageByIdQuery;
import io.spine.server.storage.jdbc.query.StorageIndexQuery;
import io.spine.server.storage.jdbc.query.WriteQueryFactory;
import io.spine.server.storage.jdbc.type.JdbcColumnType;
import org.slf4j.Logger;
import io.spine.server.entity.Entity;
import io.spine.server.entity.EntityRecord;
import io.spine.server.storage.RecordStorage;
import io.spine.server.storage.LifecycleFlagField;
import io.spine.server.storage.jdbc.entity.lifecycleflags.query.MarkEntityQuery;
import io.spine.server.storage.jdbc.query.WriteQuery;
import io.spine.server.storage.jdbc.table.entity.RecordTable;
import io.spine.server.storage.jdbc.util.DataSourceWrapper;
import io.spine.server.storage.jdbc.util.DbTableNameFactory;
import io.spine.server.storage.jdbc.util.IdColumn;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class creates queries for interaction with the {@link RecordTable}.
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
    private final ColumnTypeRegistry<? extends JdbcColumnType<?, ?>> columnTypeRegistry;

    /**
     * Creates a new instance.
     *
     * @param dataSource  instance of {@link DataSourceWrapper}
     * @param entityClass entity class of corresponding {@link RecordStorage} instance
     */
    public RecordStorageQueryFactory(DataSourceWrapper dataSource,
                                     Class<? extends Entity<I, ?>> entityClass,
                                     Logger logger,
                                     IdColumn<I> idColumn,
                                     ColumnTypeRegistry<?
                                             extends JdbcColumnType<?, ?>> columnTypeRegistry) {
        super();
        this.idColumn = checkNotNull(idColumn);
        this.dataSource = dataSource;
        this.tableName = DbTableNameFactory.newTableName(entityClass);
        this.logger = logger;
        this.columnTypeRegistry = columnTypeRegistry;
    }

    private MarkEntityQuery<I> newMarkQuery(I id, LifecycleFlagField column) {
        final MarkEntityQuery<I> query =
                MarkEntityQuery.<I>newBuilder()
                        .setDataSource(dataSource)
                        .setLogger(getLogger())
                        .setTableName(tableName)
                        .setColumn(column)
                        .setIdColumn(idColumn)
                        .setId(id)
                        .build();
        return query;
    }

    public Logger getLogger() {
        return logger;
    }

    public SelectBulkQuery<I> newSelectAllQuery(FieldMask fieldMask) {
        final SelectBulkQuery.Builder<I> builder = SelectBulkQuery.<I>newBuilder(tableName)
                .setFieldMask(fieldMask)
                .setLogger(getLogger())
                .setDataSource(dataSource);

        return builder.build();
    }

    public SelectBulkQuery<I> newSelectBulkQuery(Iterable<I> ids, FieldMask fieldMask) {
        final SelectBulkQuery.Builder<I> builder = SelectBulkQuery.<I>newBuilder()
                .setIdColumn(idColumn)
                .setIdsQuery(tableName, ids)
                .setFieldMask(fieldMask)
                .setLogger(getLogger())
                .setDataSource(dataSource);

        return builder.build();
    }

    public SelectByEntityQuery<I> newSelectByEntityQuery(EntityQuery<I> query, FieldMask fieldMask) {
        final SelectByEntityQuery.Builder<I> builder =
                SelectByEntityQuery.<I>newBuilder()
                .setQueryByEntity(query, tableName)
                .setIdColumn(idColumn)
                .setFieldMask(fieldMask)
                .setLogger(getLogger())
                .setDataSource(dataSource);

        return builder.build();
    }

    public InsertEntityRecordsBulkQuery<I> newInsertEntityRecordsBulkQuery(
            Map<I, EntityRecordWithColumns> records) {
        final InsertEntityRecordsBulkQuery.Builder<I> builder =
                InsertEntityRecordsBulkQuery.<I>newBuilder()
                        .setLogger(getLogger())
                        .setDataSource(dataSource)
                        .setTableName(tableName)
                        .setIdColumn(idColumn)
                        .setRecords(records);
        return builder.build();
    }

    @Override
    public SelectMessageByIdQuery<I, EntityRecord> newSelectByIdQuery(I id) {
        final SelectEntityByIdQuery.Builder<I> builder =
                SelectEntityByIdQuery.<I>newBuilder(tableName)
                        .setDataSource(dataSource)
                        .setLogger(getLogger())
                        .setIdColumn(idColumn)
                        .setId(id);
        return builder.build();
    }

    @Override
    public StorageIndexQuery<I> newIndexQuery() {
        return null;
    }

    @Override
    public WriteQuery newInsertQuery(I id, EntityRecordWithColumns record) {
        final InsertEntityQuery.Builder<I> builder =
                InsertEntityQuery.<I>newBuilder(tableName, record)
                .setDataSource(dataSource)
                .setLogger(getLogger())
                .setId(id)
                .setIdColumn(idColumn)
                .setRecord(record);
        return builder.build();
    }

    @Override
    public WriteQuery newUpdateQuery(I id, EntityRecordWithColumns record) {
        final UpdateEntityQuery.Builder<I> builder =
                UpdateEntityQuery.<I>newBuilder(tableName)
                .setDataSource(dataSource)
                .setLogger(getLogger())
                .setIdColumn(idColumn)
                .setId(id)
                .setRecord(record);
        return builder.build();
    }
}
