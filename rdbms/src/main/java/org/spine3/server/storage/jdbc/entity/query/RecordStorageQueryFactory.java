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
import org.spine3.server.entity.Visibility;
import org.spine3.server.storage.RecordStorage;
import org.spine3.server.storage.jdbc.entity.visibility.AbstractVisibilityHandlingStorageQueryFactory;
import org.spine3.server.storage.jdbc.entity.visibility.VisibilityHandlingStorageQueryFactory;
import org.spine3.server.storage.jdbc.entity.visibility.VisibilityQueryFactories;
import org.spine3.server.storage.jdbc.entity.visibility.query.CreateVisibilityTableQuery;
import org.spine3.server.storage.jdbc.entity.visibility.query.InsertAndMarkEntityQuery;
import org.spine3.server.storage.jdbc.entity.visibility.query.InsertVisibilityQuery;
import org.spine3.server.storage.jdbc.entity.visibility.query.MarkEntityQuery;
import org.spine3.server.storage.jdbc.entity.visibility.query.SelectVisibilityQuery;
import org.spine3.server.storage.jdbc.entity.visibility.query.UpdateVisibilityQuery;
import org.spine3.server.storage.jdbc.query.DeleteRecordQuery;
import org.spine3.server.storage.jdbc.query.QueryFactory;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.DbTableNameFactory;
import org.spine3.server.storage.jdbc.util.IdColumn;

import java.util.Map;

import static org.spine3.server.storage.jdbc.entity.query.EntityTable.ID_COL;

/**
 * This class creates queries for interaction with {@link EntityTable}.
 *
 * @author Andrey Lavrov
 * @author Dmytro Dashenkov
 */
public class RecordStorageQueryFactory<I> extends AbstractVisibilityHandlingStorageQueryFactory<I>
        implements QueryFactory{

    private final IdColumn<I> idColumn;
    private final DataSourceWrapper dataSource;
    private final String tableName;
    private final VisibilityHandlingStorageQueryFactory<I> statusTableQueryFactory;

    /**
     * Creates a new instance.
     *
     * @param dataSource  instance of {@link DataSourceWrapper}
     * @param entityClass entity class of corresponding {@link RecordStorage} instance
     */
    public RecordStorageQueryFactory(DataSourceWrapper dataSource,
                                     Class<? extends Entity<I, ?>> entityClass) {
        super();
        this.idColumn = IdColumn.newInstance(entityClass);
        this.dataSource = dataSource;
        this.tableName = DbTableNameFactory.newTableName(entityClass);
        this.statusTableQueryFactory = VisibilityQueryFactories.forTable(dataSource,
                                                                         tableName,
                                                                         idColumn);
    }

    @Override
    public CreateVisibilityTableQuery newCreateVisibilityTableQuery() {
        return statusTableQueryFactory.newCreateVisibilityTableQuery();
    }

    @Override
    public InsertVisibilityQuery newInsertVisibilityQuery(I id, Visibility visibility) {
        return statusTableQueryFactory.newInsertVisibilityQuery(id, visibility);
    }

    @Override
    public SelectVisibilityQuery newSelectVisibilityQuery(I id) {
        return statusTableQueryFactory.newSelectVisibilityQuery(id);
    }

    @Override
    public UpdateVisibilityQuery newUpdateVisibilityQuery(I id, Visibility visibility) {
        return statusTableQueryFactory.newUpdateVisibilityQuery(id, visibility);
    }

    @Override
    public MarkEntityQuery<I> newMarkArchivedQuery(I id) {
        return statusTableQueryFactory.newMarkArchivedQuery(id);
    }

    @Override
    public MarkEntityQuery<I> newMarkDeletedQuery(I id) {
        return statusTableQueryFactory.newMarkDeletedQuery(id);
    }

    @Override
    public InsertAndMarkEntityQuery<I> newMarkArchivedNewEntityQuery(I id) {
        throw new UnsupportedOperationException("The record must be present to mark it archived.");
    }

    @Override
    public InsertAndMarkEntityQuery<I> newMarkDeletedNewEntityQuery(I id) {
        throw new UnsupportedOperationException("The record must be present to mark it deleted.");
    }

    /** Sets the logger for logging exceptions during queries execution. */
    @Override
    public void setLogger(Logger logger) {
        super.setLogger(logger);
        if (statusTableQueryFactory instanceof AbstractVisibilityHandlingStorageQueryFactory) {
            // Overriding implementations might want to use their own query factory
            final AbstractVisibilityHandlingStorageQueryFactory<I> abstractQueryFactory =
                    (AbstractVisibilityHandlingStorageQueryFactory<I>) statusTableQueryFactory;
            abstractQueryFactory.setLogger(logger);
        }
    }

    /** Returns a query that creates a new {@link EntityTable} if it does not exist. */
    public CreateEntityTableQuery newCreateEntityTableQuery() {
        final CreateEntityTableQuery.Builder<I> builder =
                CreateEntityTableQuery.<I>newBuilder()
                                      .setDataSource(dataSource)
                                      .setLogger(getLogger())
                                      .setIdColumn(idColumn)
                                      .setTableName(tableName);
        return builder.build();
    }

    /**
     * Returns a query that inserts a new {@link EntityRecord} to the {@link EntityTable}.
     *
     * @param id     new entity record id
     * @param record new entity record
     */
    public InsertEntityQuery newInsertEntityQuery(I id, EntityRecord record) {
        final InsertEntityQuery.Builder<I> builder = InsertEntityQuery.<I>newBuilder(tableName)
                                                                      .setDataSource(dataSource)
                                                                      .setLogger(getLogger())
                                                                      .setId(id)
                                                                      .setIdColumn(idColumn)
                                                                      .setRecord(record);
        return builder.build();
    }

    /**
     * Returns a query that updates {@link EntityRecord} in the {@link EntityTable}.
     *
     * @param id     entity id
     * @param record updated record state
     */
    public UpdateEntityQuery newUpdateEntityQuery(I id, EntityRecord record) {
        final UpdateEntityQuery.Builder<I> builder = UpdateEntityQuery.<I>newBuilder(tableName)
                                                                      .setDataSource(dataSource)
                                                                      .setLogger(getLogger())
                                                                      .setIdColumn(idColumn)
                                                                      .setId(id)
                                                                      .setRecord(record);
        return builder.build();
    }

    /** Returns a query that selects {@link EntityRecord} by ID. */
    public SelectEntityByIdQuery<I> newSelectEntityByIdQuery(I id) {
        final SelectEntityByIdQuery.Builder<I> builder =
                SelectEntityByIdQuery.<I>newBuilder(tableName)
                                     .setDataSource(dataSource)
                                     .setLogger(getLogger())
                                     .setIdColumn(idColumn)
                                     .setId(id);
        return builder.build();
    }

    public DeleteRecordQuery<I> newDeleteRowQuery(I id) {
        final DeleteRecordQuery.Builder<I> builder = DeleteRecordQuery.<I>newBuilder()
                                                                .setDataSource(dataSource)
                                                                .setLogger(getLogger())
                                                                .setTableName(tableName)
                                                                .setIdColumn(idColumn)
                                                                .setIdColumnName(ID_COL)
                                                                .setIdValue(id);
        return builder.build();
    }

    /** Returns a query that deletes all from {@link EntityTable}. */
    public DeleteAllQuery newDeleteAllQuery() {
        final DeleteAllQuery.Builder builder = DeleteAllQuery.newBuilder(tableName)
                                                             .setDataSource(dataSource)
                                                             .setLogger(getLogger());
        return builder.build();
    }

    public SelectBulkQuery newSelectAllQuery(FieldMask fieldMask) {
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

    public InsertEntityRecordsBulkQuery<I> newInsertEntityRecordsBulkQuery(
            Map<I, EntityRecord> records) {
        final InsertEntityRecordsBulkQuery.Builder<I> builder =
                InsertEntityRecordsBulkQuery.<I>newBuilder()
                                            .setLogger(getLogger())
                                            .setDataSource(dataSource)
                                            .setTableName(tableName)
                                            .setidColumn(idColumn)
                                            .setRecords(records);
        return builder.build();
    }
}
