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

package org.spine3.server.storage.jdbc.entity.visibility;

import org.slf4j.Logger;
import org.spine3.Internal;
import org.spine3.server.entity.Visibility;
import org.spine3.server.storage.VisibilityField;
import org.spine3.server.storage.jdbc.entity.visibility.query.InsertAndMarkEntityQuery;
import org.spine3.server.storage.jdbc.entity.visibility.query.MarkEntityQuery;
import org.spine3.server.storage.jdbc.entity.visibility.query.CreateVisibilityTableQuery;
import org.spine3.server.storage.jdbc.entity.visibility.query.InsertVisibilityQuery;
import org.spine3.server.storage.jdbc.entity.visibility.query.SelectVisibilityQuery;
import org.spine3.server.storage.jdbc.entity.visibility.query.UpdateVisibilityQuery;
import org.spine3.server.storage.jdbc.entity.visibility.table.VisibilityTable;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.spine3.server.storage.VisibilityField.archived;
import static org.spine3.server.storage.VisibilityField.deleted;

/**
 * @author Dmytro Dashenkov.
 */
@Internal
public class VisibilityHandlingStorageQueryFactoryImpl<I> implements VisibilityHandlingStorageQueryFactory<I> {

    private final DataSourceWrapper dataSource;
    private final String tableName;
    private final IdColumn<I> idColumn;
    private Logger logger;

    VisibilityHandlingStorageQueryFactoryImpl(DataSourceWrapper dataSource,
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
    public CreateVisibilityTableQuery newCreateVisibilityTableQuery() {
        final CreateVisibilityTableQuery.Builder builder =
                CreateVisibilityTableQuery.newBuilder()
                                            .setDataSource(dataSource)
                                            .setLogger(logger)
                                            .setTableName(VisibilityTable.TABLE_NAME);
        return builder.build();
    }

    @Override
    public InsertVisibilityQuery newInsertVisibilityQuery(I id, Visibility visibility) {
        final InsertVisibilityQuery.Builder builder =
                InsertVisibilityQuery.newBuilder()
                                       .setDataSource(dataSource)
                                       .setLogger(logger)
                                       .setId(id)
                                       .setVisibility(visibility);
        return builder.build();
    }

    @Override
    public SelectVisibilityQuery newSelectVisibilityQuery(I id) {
        final SelectVisibilityQuery.Builder builder =
                SelectVisibilityQuery.newBuilder()
                                       .setDataSource(dataSource)
                                       .setLogger(logger)
                                       .setId(id);
        return builder.build();
    }

    @Override
    public UpdateVisibilityQuery newUpdateVisibilityQuery(I id, Visibility visibility) {
        final UpdateVisibilityQuery.Builder builder = UpdateVisibilityQuery.<I>newBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setId(id)
                .setVisibility(visibility);
        return builder.build();
    }

    @Override
    public MarkEntityQuery<I> newMarkArchivedQuery(I id) {
        return newMarkQuery(id, archived);
    }

    @Override
    public MarkEntityQuery<I> newMarkDeletedQuery(I id) {
        return newMarkQuery(id, deleted);
    }

    @Override
    public InsertAndMarkEntityQuery<I> newMarkArchivedNewEntityQuery(I id) {
        return newInsertAndMarkEntityQuery(id, archived);
    }

    @Override
    public InsertAndMarkEntityQuery<I> newMarkDeletedNewEntityQuery(I id) {
        return newInsertAndMarkEntityQuery(id, deleted);
    }

    private InsertAndMarkEntityQuery<I> newInsertAndMarkEntityQuery(I id, VisibilityField column) {
        final InsertAndMarkEntityQuery<I> query = InsertAndMarkEntityQuery.<I>newInsertBuilder()
                .setDataSource(dataSource)
                .setLogger(logger)
                .setTableName(tableName)
                .setColumn(column)
                .setIdColumn(idColumn)
                .setId(id)
                .build();
        return query;
    }

    private MarkEntityQuery<I> newMarkQuery(I id, VisibilityField column) {
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
