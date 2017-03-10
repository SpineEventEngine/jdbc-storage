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

package org.spine3.server.storage.jdbc.entity.query;

import com.google.protobuf.FieldMask;
import org.slf4j.Logger;
import org.spine3.server.entity.Entity;
import org.spine3.server.entity.EntityRecord;
import org.spine3.server.storage.RecordStorage;
import org.spine3.server.storage.VisibilityField;
import org.spine3.server.storage.jdbc.entity.visibility.query.MarkEntityQuery;
import org.spine3.server.storage.jdbc.query.QueryFactory;
import org.spine3.server.storage.jdbc.query.SelectByIdQuery;
import org.spine3.server.storage.jdbc.query.WriteQuery;
import org.spine3.server.storage.jdbc.table.entity.RecordTable;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.DbTableNameFactory;
import org.spine3.server.storage.jdbc.util.IdColumnSetter;

import java.util.Map;

/**
 * This class creates queries for interaction with the {@link RecordTable}.
 *
 * @author Andrey Lavrov
 * @author Dmytro Dashenkov
 */
public class RecordStorageQueryFactory<I> implements QueryFactory<I,EntityRecord> {

    private final IdColumnSetter<I> idColumnSetter;
    private final DataSourceWrapper dataSource;
    private final String tableName;
    private final Logger logger;

    /**
     * Creates a new instance.
     *
     * @param dataSource  instance of {@link DataSourceWrapper}
     * @param entityClass entity class of corresponding {@link RecordStorage} instance
     */
    public RecordStorageQueryFactory(DataSourceWrapper dataSource,
                                     Class<? extends Entity<I, ?>> entityClass,
                                     Logger logger) {
        super();
        this.idColumnSetter = IdColumnSetter.newInstance(entityClass);
        this.dataSource = dataSource;
        this.tableName = DbTableNameFactory.newTableName(entityClass);
        this.logger = logger;
    }

    public MarkEntityQuery<I> newMarkArchivedQuery(I id) {
        return newMarkQuery(id, VisibilityField.archived);
    }

    public MarkEntityQuery<I> newMarkDeletedQuery(I id) {
        return newMarkQuery(id, VisibilityField.deleted);
    }

    private MarkEntityQuery<I> newMarkQuery(I id, VisibilityField column) {
        final MarkEntityQuery<I> query = MarkEntityQuery.<I>newBuilder()
                .setDataSource(dataSource)
                .setLogger(getLogger())
                .setTableName(tableName)
                .setColumn(column)
                .setIdColumnSetter(idColumnSetter)
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
                                                                  .setIdColumnSetter(
                                                                          idColumnSetter)
                                                                  .setIdsQuery(tableName, ids)
                                                                  .setFieldMask(fieldMask)
                                                                  .setLogger(getLogger())
                                                                  .setDataSource(dataSource);

        return builder.build();
    }

    public InsertEntityRecordsBulkQuery<I> newInsertEntityRecordsBulkQuery(
            Map<I, EntityRecord> records) {
        final InsertEntityRecordsBulkQuery.Builder<I> builder =
                InsertEntityRecordsBulkQuery.<I>newBuilder()
                                            .setLogger(getLogger())
                                            .setDataSource(dataSource)
                                            .setTableName(tableName)
                                            .setidColumn(idColumnSetter)
                                            .setRecords(records);
        return builder.build();
    }

    @Override
    public SelectByIdQuery<I, EntityRecord> newSelectByIdQuery(I id) {
        final SelectEntityByIdQuery.Builder<I> builder =
                SelectEntityByIdQuery.<I>newBuilder(tableName)
                        .setDataSource(dataSource)
                        .setLogger(getLogger())
                        .setIdColumnSetter(idColumnSetter)
                        .setId(id);
        return builder.build();
    }

    @Override
    public WriteQuery newInsertQuery(I id, EntityRecord record) {
        final InsertEntityQuery.Builder<I> builder = InsertEntityQuery.<I>newBuilder(tableName)
                .setDataSource(dataSource)
                .setLogger(getLogger())
                .setId(id)
                .setIdColumnSetter(idColumnSetter)
                .setRecord(record);
        return builder.build();
    }

    @Override
    public WriteQuery newUpdateQuery(I id, EntityRecord record) {
        final UpdateEntityQuery.Builder<I> builder = UpdateEntityQuery.<I>newBuilder(tableName)
                .setDataSource(dataSource)
                .setLogger(getLogger())
                .setIdColumnSetter(idColumnSetter)
                .setId(id)
                .setRecord(record);
        return builder.build();
    }
}
