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

package org.spine3.server.storage.jdbc.aggregate.query;

import org.slf4j.Logger;
import org.spine3.server.entity.Visibility;
import org.spine3.server.storage.VisibilityField;
import org.spine3.server.storage.jdbc.entity.visibility.query.InsertAndMarkEntityQuery;
import org.spine3.server.storage.jdbc.entity.visibility.query.InsertVisibilityQuery;
import org.spine3.server.storage.jdbc.entity.visibility.query.MarkEntityQuery;
import org.spine3.server.storage.jdbc.entity.visibility.query.SelectVisibilityQuery;
import org.spine3.server.storage.jdbc.entity.visibility.query.UpdateVisibilityQuery;
import org.spine3.server.storage.jdbc.query.QueryFactory;
import org.spine3.server.storage.jdbc.query.SelectByIdQuery;
import org.spine3.server.storage.jdbc.query.WriteQuery;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import static org.spine3.server.storage.VisibilityField.archived;
import static org.spine3.server.storage.VisibilityField.deleted;

/**
 * An implementation of the {@link QueryFactory} for generating queries fot
 * the {@link org.spine3.server.storage.jdbc.table.entity.aggregate.VisibilityTable}.
 *
 * @author Dmytro Dashenkov
 */
public class VisibilityQueryFactory<I> implements QueryFactory<I, Visibility> {

    private final Logger logger;
    private final DataSourceWrapper dataSource;
    private final IdColumn<I> idColumn;
    private final String tableName;

    public VisibilityQueryFactory(DataSourceWrapper dataSource,
                                  Logger logger,
                                  IdColumn<I> idColumn,
                                  String tableName) {
        this.logger = logger;
        this.dataSource = dataSource;
        this.idColumn = idColumn;
        this.tableName = tableName;
    }

    @Override
    public SelectByIdQuery<I, Visibility> newSelectByIdQuery(I id) {
        final SelectByIdQuery<I, Visibility> query =
                SelectVisibilityQuery.<I>newBuilder(tableName)
                                     .setDataSource(dataSource)
                                     .setLogger(logger)
                                     .setIdColumn(idColumn)
                                     .setId(id)
                                     .build();
        return query;
    }

    @Override
    public WriteQuery newInsertQuery(I id, Visibility record) {
        final WriteQuery query = InsertVisibilityQuery.<I>newBuilder(tableName)
                                                      .setId(id)
                                                      .setVisibility(record)
                                                      .setLogger(logger)
                                                      .setDataSource(dataSource)
                                                      .setIdColumn(idColumn)
                                                      .build();
        return query;
    }

    @Override
    public WriteQuery newUpdateQuery(I id, Visibility record) {
        final WriteQuery query = UpdateVisibilityQuery.<I>newBuilder(tableName)
                                                      .setLogger(logger)
                                                      .setDataSource(dataSource)
                                                      .setId(id)
                                                      .setVisibility(record)
                                                      .setIdColumn(idColumn)
                                                      .build();
        return query;
    }

    public MarkEntityQuery<I> newMarkArchivedQuery(I id) {
        return newMarkQuery(id, archived);
    }

    public MarkEntityQuery<I> newMarkDeletedQuery(I id) {
        return newMarkQuery(id, deleted);
    }

    public InsertAndMarkEntityQuery<I> newMarkArchivedNewEntityQuery(I id) {
        return newInsertAndMarkEntityQuery(id, archived);
    }

    public InsertAndMarkEntityQuery<I> newMarkDeletedNewEntityQuery(I id) {
        return newInsertAndMarkEntityQuery(id, deleted);
    }

    private InsertAndMarkEntityQuery<I> newInsertAndMarkEntityQuery(I id,
                                                                         VisibilityField column) {
        final InsertAndMarkEntityQuery<I> query =
                InsertAndMarkEntityQuery.<I>newInsertBuilder()
                                        .setDataSource(dataSource)
                                        .setTableName(tableName)
                                        .setLogger(logger)
                                        .setColumn(column)
                                        .setIdColumn(idColumn)
                                        .setId(id)
                                        .build();
        return query;
    }

    private MarkEntityQuery<I> newMarkQuery(I id, VisibilityField column) {
        final MarkEntityQuery<I> query =
                MarkEntityQuery.<I>newBuilder()
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
