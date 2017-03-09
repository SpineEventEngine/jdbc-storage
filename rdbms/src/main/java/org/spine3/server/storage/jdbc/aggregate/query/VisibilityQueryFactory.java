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
import org.spine3.server.storage.jdbc.entity.visibility.table.VisibilityTable;
import org.spine3.server.storage.jdbc.query.QueryFactory;
import org.spine3.server.storage.jdbc.query.SelectByIdQuery;
import org.spine3.server.storage.jdbc.query.WriteQuery;
import org.spine3.server.storage.jdbc.util.DataSourceWrapper;
import org.spine3.server.storage.jdbc.util.IdColumn;

import static org.spine3.server.storage.VisibilityField.archived;
import static org.spine3.server.storage.VisibilityField.deleted;

/**
 * @author Dmytro Dashenkov.
 */
public class VisibilityQueryFactory implements QueryFactory<String, Visibility> {

    private final Logger logger;
    private final DataSourceWrapper dataSource;

    public VisibilityQueryFactory(DataSourceWrapper dataSource, Logger logger) {
        this.logger = logger;
        this.dataSource = dataSource;
    }

    @Override
    public SelectByIdQuery<String, Visibility> newSelectByIdQuery(String id) {
        final SelectByIdQuery<String, Visibility> query =
                SelectVisibilityQuery.<String>newBuilder()
                                     .setDataSource(dataSource)
                                     .setLogger(logger)
                                     .setIdColumn(new IdColumn.StringIdColumn())
                                     .setId(id)
                                     .build();
        return query;
    }

    @Override
    public WriteQuery newInsertQuery(String id, Visibility record) {
        final WriteQuery query = InsertVisibilityQuery.newBuilder()
                                                      .setId(id)
                                                      .setVisibility(record)
                                                      .setLogger(logger)
                                                      .setDataSource(dataSource)
                                                      .build();
        return query;
    }

    @Override
    public WriteQuery newUpdateQuery(String id, Visibility record) {
        final WriteQuery query = UpdateVisibilityQuery.newBuilder()
                                                      .setLogger(logger)
                                                      .setDataSource(dataSource)
                                                      .setId(id)
                                                      .setVisibility(record)
                                                      .build();
        return query;
    }

    public MarkEntityQuery<String> newMarkArchivedQuery(String id) {
        return newMarkQuery(id, archived);
    }

    public MarkEntityQuery<String> newMarkDeletedQuery(String id) {
        return newMarkQuery(id, deleted);
    }

    public InsertAndMarkEntityQuery<String> newMarkArchivedNewEntityQuery(String id) {
        return newInsertAndMarkEntityQuery(id, archived);
    }

    public InsertAndMarkEntityQuery<String> newMarkDeletedNewEntityQuery(String id) {
        return newInsertAndMarkEntityQuery(id, deleted);
    }

    private InsertAndMarkEntityQuery<String> newInsertAndMarkEntityQuery(String id,
                                                                         VisibilityField column) {
        final InsertAndMarkEntityQuery<String> query =
                InsertAndMarkEntityQuery.<String>newInsertBuilder()
                                        .setDataSource(dataSource)
                                        .setLogger(logger)
                                        .setTableName(VisibilityTable.TABLE_NAME)
                                        .setColumn(column)
                                        .setIdColumn(new IdColumn.StringIdColumn())
                                        .setId(id)
                                        .build();
        return query;
    }

    private MarkEntityQuery<String> newMarkQuery(String id, VisibilityField column) {
        final MarkEntityQuery<String> query =
                MarkEntityQuery.<String>newBuilder()
                               .setDataSource(dataSource)
                               .setLogger(logger)
                               .setTableName(VisibilityTable.TABLE_NAME)
                               .setColumn(column)
                               .setIdColumn(new IdColumn.StringIdColumn())
                               .setId(id)
                               .build();
        return query;
    }
}
