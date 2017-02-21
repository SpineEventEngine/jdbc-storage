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

package org.spine3.server.storage.jdbc.entity.status;

import org.slf4j.Logger;
import org.spine3.Internal;
import org.spine3.server.entity.status.EntityStatus;
import org.spine3.server.storage.EntityStatusField;
import org.spine3.server.storage.jdbc.entity.query.MarkEntityQuery;
import org.spine3.server.storage.jdbc.entity.status.query.CreateEntityStatusTableQuery;
import org.spine3.server.storage.jdbc.entity.status.query.InsertEntityStatusQuery;
import org.spine3.server.storage.jdbc.entity.status.query.SelectEntityStatusQuery;
import org.spine3.server.storage.jdbc.entity.status.query.UpdateEntityStatusQuery;
import org.spine3.server.storage.jdbc.entity.status.table.EntityStatusTable;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Dmytro Dashenkov.
 */
@Internal
public class EntityStatusHandlingStorageQueryFactoryImpl<I> implements EntityStatusHandlingStorageQueryFactory<I> {

    private final DataSourceWrapper dataSource;
    private final String tableName;
    private final IdColumn<I> idColumn;
    private Logger logger;

    EntityStatusHandlingStorageQueryFactoryImpl(DataSourceWrapper dataSource,
                                                       String tableName,
                                                       IdColumn<I> idColumn) {
        this.dataSource = checkNotNull(dataSource);
        this.tableName = checkNotNull(tableName);
        this.idColumn = checkNotNull(idColumn);
    }

    @Override
    public void setLogger(Logger logger) {
        this.logger = checkNotNull(logger);
    }

    @Override
    public CreateEntityStatusTableQuery newCreateEntityStatusTableQuery() {
        final CreateEntityStatusTableQuery.Builder builder =
                CreateEntityStatusTableQuery.newBuilder()
                                            .setDataSource(dataSource)
                                            .setLogger(logger)
                                            .setTableName(EntityStatusTable.TABLE_NAME);
        return builder.build();
    }

    @Override
    public InsertEntityStatusQuery newInsertEntityStatusQuery(I id, EntityStatus entityStatus) {
        final InsertEntityStatusQuery.Builder builder =
                InsertEntityStatusQuery.newBuilder()
                                       .setDataSource(dataSource)
                                       .setLogger(logger)
                                       .setId(id)
                                       .setEntityStatus(entityStatus);
        return builder.build();
    }

    @Override
    public SelectEntityStatusQuery newSelectEntityStatusQuery(I id) {
        final SelectEntityStatusQuery.Builder builder =
                SelectEntityStatusQuery.newBuilder()
                                       .setDataSource(dataSource)
                                       .setLogger(logger)
                                       .setId(id);
        return builder.build();
    }

    @Override
    public UpdateEntityStatusQuery newUpdateEntityStatusQuery(I id, EntityStatus status) {
        final UpdateEntityStatusQuery.Builder builder = UpdateEntityStatusQuery.<I>newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setId(id)
                .setEntityStatus(status);
        return builder.build();
    }

    @Override
    public MarkEntityQuery<I> newMarkArchivedQuery(I id) {
        return newMarkQuery(id, EntityStatusField.archived);
    }

    @Override
    public MarkEntityQuery<I> newMarkDeletedQuery(I id) {
        return newMarkQuery(id, EntityStatusField.deleted);
    }

    private MarkEntityQuery<I> newMarkQuery(I id, EntityStatusField column) {
        final MarkEntityQuery<I> query = MarkEntityQuery.<I>newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setTableName(tableName)
                .setColumn(column)
                .setIdColumn(idColumn)
                .setId(id)
                .build();
        return query;
    }
}
